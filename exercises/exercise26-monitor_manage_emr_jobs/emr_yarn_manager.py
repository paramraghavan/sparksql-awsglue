#!/usr/bin/env python3
"""
EMR YARN Job Manager
====================
This script allows you to:
1. List all active EMR clusters
2. View YARN jobs running on each cluster
3. Kill selected YARN jobs

Requirements:
- boto3: pip install boto3
- AWS credentials configured (via ~/.aws/credentials, environment variables, or IAM role)
- SSH access to EMR master nodes (for YARN operations)

Usage:
    python emr_yarn_manager.py
"""

import boto3
import subprocess
import sys
import json
from datetime import datetime
from typing import Optional


class EMRYarnManager:
    def __init__(self, region: str = None, profile: str = None):
        """
        Initialize EMR client with optional region and profile override.

        Args:
            region: AWS region (e.g., 'us-east-1')
            profile: AWS profile name from ~/.aws/credentials
        """
        self.session = boto3.Session(profile_name=profile, region_name=region)
        self.emr_client = self.session.client('emr')
        self.region = self.session.region_name or 'us-east-1'
        self.profile = profile

    def get_active_clusters(self) -> list:
        """
        Get all active EMR clusters.
        Active states: STARTING, BOOTSTRAPPING, RUNNING, WAITING
        """
        active_states = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        clusters = []

        try:
            paginator = self.emr_client.get_paginator('list_clusters')
            for page in paginator.paginate(ClusterStates=active_states):
                for cluster in page.get('Clusters', []):
                    clusters.append({
                        'id': cluster['Id'],
                        'name': cluster['Name'],
                        'state': cluster['Status']['State'],
                        'created': cluster['Status']['Timeline'].get('CreationDateTime', 'N/A')
                    })
        except Exception as e:
            print(f"Error fetching clusters: {e}")

        return clusters

    def get_cluster_details(self, cluster_id: str) -> dict:
        """Get detailed information about a specific cluster."""
        try:
            response = self.emr_client.describe_cluster(ClusterId=cluster_id)
            cluster = response['Cluster']

            # Get master public DNS
            master_dns = cluster.get('MasterPublicDnsName', 'N/A')

            # Get master instance ID for SSM
            master_instance_id = None
            try:
                instances = self.emr_client.list_instances(
                    ClusterId=cluster_id,
                    InstanceGroupTypes=['MASTER']
                )
                if instances.get('Instances'):
                    master_instance_id = instances['Instances'][0].get('Ec2InstanceId')
            except Exception:
                pass

            return {
                'id': cluster['Id'],
                'name': cluster['Name'],
                'state': cluster['Status']['State'],
                'master_dns': master_dns,
                'master_instance_id': master_instance_id,
                'applications': [app['Name'] for app in cluster.get('Applications', [])],
                'instance_hours': cluster.get('NormalizedInstanceHours', 0),
                'release_label': cluster.get('ReleaseLabel', 'N/A')
            }
        except Exception as e:
            print(f"Error fetching cluster details: {e}")
            return {}

    def get_yarn_applications_via_ssm(self, instance_id: str) -> list:
        """
        Get YARN applications using AWS Systems Manager (SSM).
        No SSH key required - uses IAM permissions.

        Args:
            instance_id: EC2 instance ID of the master node
        """
        applications = []

        if not instance_id:
            print("  [SSM] Master instance ID not available")
            return applications

        try:
            ssm_client = self.session.client('ssm')

            print(f"  [SSM] Sending command to instance {instance_id}")

            response = ssm_client.send_command(
                InstanceIds=[instance_id],
                DocumentName='AWS-RunShellScript',
                Parameters={
                    'commands': ['sudo -u hadoop yarn application -list -appStates RUNNING,ACCEPTED,SUBMITTED,NEW 2>/dev/null || sudo -u hadoop yarn application -list 2>/dev/null']
                },
                TimeoutSeconds=30
            )

            command_id = response['Command']['CommandId']
            print(f"  [SSM] Command ID: {command_id}")

            # Wait for command to complete
            import time
            for _ in range(10):
                time.sleep(2)
                result = ssm_client.get_command_invocation(
                    CommandId=command_id,
                    InstanceId=instance_id
                )

                if result['Status'] in ['Success', 'Failed', 'TimedOut', 'Cancelled']:
                    break

            print(f"  [SSM] Status: {result['Status']}")

            if result['Status'] == 'Success':
                output = result.get('StandardOutputContent', '')
                print(f"  [SSM] Output: {output[:1000] if output else '(empty)'}")

                for line in output.strip().split('\n'):
                    if line.strip() and not line.startswith('Total') and 'Application-Id' not in line and '===' not in line:
                        parts = line.split()
                        if len(parts) >= 6 and parts[0].startswith('application_'):
                            applications.append({
                                'id': parts[0],
                                'name': parts[1],
                                'type': parts[2],
                                'user': parts[3],
                                'queue': parts[4],
                                'state': parts[5],
                                'final_status': parts[6] if len(parts) > 6 else 'N/A',
                                'progress': parts[7] if len(parts) > 7 else 'N/A'
                            })
            else:
                print(f"  [SSM] Error: {result.get('StandardErrorContent', 'Unknown error')}")

        except Exception as e:
            print(f"  [SSM] Error: {e}")
            print("  [SSM] Make sure SSM agent is installed and IAM role has SSM permissions")

        return applications

    def kill_yarn_application_via_ssm(self, instance_id: str, app_id: str) -> bool:
        """Kill a YARN application using SSM."""
        if not instance_id:
            print("Master instance ID not available.")
            return False

        try:
            ssm_client = self.session.client('ssm')

            response = ssm_client.send_command(
                InstanceIds=[instance_id],
                DocumentName='AWS-RunShellScript',
                Parameters={
                    'commands': [f'sudo -u hadoop yarn application -kill {app_id}']
                },
                TimeoutSeconds=30
            )

            command_id = response['Command']['CommandId']

            import time
            for _ in range(10):
                time.sleep(2)
                result = ssm_client.get_command_invocation(
                    CommandId=command_id,
                    InstanceId=instance_id
                )
                if result['Status'] in ['Success', 'Failed', 'TimedOut', 'Cancelled']:
                    break

            if result['Status'] == 'Success':
                print(f"Successfully killed application: {app_id}")
                return True
            else:
                print(f"Failed to kill application: {result.get('StandardErrorContent', 'Unknown error')}")
                return False

        except Exception as e:
            print(f"Error killing application via SSM: {e}")
            return False

    def get_yarn_applications_via_api(self, cluster_id: str) -> list:
        """
        Get YARN applications using AWS EMR API (no SSH required).
        Note: This only works for EMR clusters with YARN visible to EMR API.
        """
        applications = []
        try:
            # Using EMR step to list YARN apps (alternative approach)
            response = self.emr_client.list_steps(
                ClusterId=cluster_id,
                StepStates=['PENDING', 'RUNNING']
            )
            for step in response.get('Steps', []):
                applications.append({
                    'id': step['Id'],
                    'name': step['Name'],
                    'state': step['Status']['State'],
                    'type': 'EMR Step'
                })
        except Exception as e:
            print(f"Error fetching steps: {e}")

        return applications

    def get_yarn_applications_via_ssh(self, master_dns: str, key_file: str = None) -> list:
        """
        Get YARN applications by SSHing to master node.

        Args:
            master_dns: Master node public DNS
            key_file: Path to SSH key file (optional, uses default if not provided)
        """
        applications = []

        if not master_dns or master_dns == 'N/A':
            print("  [SSH] Master DNS not available. Cluster might not be fully running.")
            return applications

        # Build SSH command - list all apps including running
        ssh_cmd = ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ConnectTimeout=10']

        if key_file:
            ssh_cmd.extend(['-i', key_file])

        # Get all applications (not just running)
        yarn_command = 'yarn application -list -appStates ALL 2>/dev/null || yarn application -list 2>/dev/null'

        ssh_cmd.extend([
            f'hadoop@{master_dns}',
            yarn_command
        ])

        print(f"  [SSH] Connecting to hadoop@{master_dns}")
        print(f"  [SSH] Command: {yarn_command}")

        try:
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )

            print(f"  [SSH] Return code: {result.returncode}")
            print(f"  [SSH] Stdout: {result.stdout[:1000] if result.stdout else '(empty)'}")
            if result.stderr:
                print(f"  [SSH] Stderr: {result.stderr[:500]}")

            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                # Skip header lines (typically first 2 lines)
                for line in lines:
                    if line.strip() and not line.startswith('Total') and 'Application-Id' not in line and '===' not in line:
                        parts = line.split()
                        if len(parts) >= 6 and parts[0].startswith('application_'):
                            applications.append({
                                'id': parts[0],
                                'name': parts[1],
                                'type': parts[2],
                                'user': parts[3],
                                'queue': parts[4],
                                'state': parts[5],
                                'final_status': parts[6] if len(parts) > 6 else 'N/A',
                                'progress': parts[7] if len(parts) > 7 else 'N/A'
                            })
            else:
                print(f"  [SSH] Command failed")

        except subprocess.TimeoutExpired:
            print("  [SSH] Connection timed out")
        except FileNotFoundError:
            print("  [SSH] SSH not available. Install OpenSSH client.")
        except Exception as e:
            print(f"  [SSH] Error: {e}")

        return applications

    def get_yarn_applications_via_rest(self, master_dns: str, port: int = 8088) -> list:
        """
        Get YARN applications via ResourceManager REST API.
        Requires network access to the master node on port 8088.
        """
        import urllib.request
        import urllib.error

        applications = []

        if not master_dns or master_dns == 'N/A':
            print("  [REST] Master DNS not available")
            return applications

        # Try all active states
        url = f"http://{master_dns}:{port}/ws/v1/cluster/apps?states=NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING"
        print(f"  [REST] Trying URL: {url}")

        try:
            req = urllib.request.Request(url)
            req.add_header('Accept', 'application/json')

            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())
                print(f"  [REST] Response received: {json.dumps(data, indent=2)[:500]}...")

                if data.get('apps') and data['apps'].get('app'):
                    for app in data['apps']['app']:
                        applications.append({
                            'id': app.get('id', 'N/A'),
                            'name': app.get('name', 'N/A'),
                            'type': app.get('applicationType', 'N/A'),
                            'user': app.get('user', 'N/A'),
                            'queue': app.get('queue', 'N/A'),
                            'state': app.get('state', 'N/A'),
                            'final_status': app.get('finalStatus', 'N/A'),
                            'progress': f"{app.get('progress', 0):.1f}%"
                        })
                else:
                    print("  [REST] No apps in response (apps field empty or null)")

        except urllib.error.URLError as e:
            print(f"  [REST] Cannot connect to YARN ResourceManager: {e}")
            print("  [REST] Make sure the cluster's security group allows access to port 8088")
        except Exception as e:
            print(f"  [REST] Error fetching YARN apps: {e}")

        return applications

    def kill_yarn_application_via_ssh(self, master_dns: str, app_id: str, key_file: str = None) -> bool:
        """
        Kill a YARN application by SSHing to master node.

        Args:
            master_dns: Master node public DNS
            app_id: YARN application ID (e.g., application_1234567890123_0001)
            key_file: Path to SSH key file
        """
        if not master_dns or master_dns == 'N/A':
            print("Master DNS not available.")
            return False

        ssh_cmd = ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ConnectTimeout=10']

        if key_file:
            ssh_cmd.extend(['-i', key_file])

        ssh_cmd.extend([
            f'hadoop@{master_dns}',
            f'yarn application -kill {app_id}'
        ])

        try:
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0 or 'Killed application' in result.stdout:
                print(f"Successfully killed application: {app_id}")
                return True
            else:
                print(f"Failed to kill application: {result.stderr}")
                return False

        except Exception as e:
            print(f"Error killing application: {e}")
            return False

    def cancel_emr_step(self, cluster_id: str, step_id: str) -> bool:
        """Cancel an EMR step."""
        try:
            self.emr_client.cancel_steps(
                ClusterId=cluster_id,
                StepIds=[step_id],
                StepCancellationOption='SEND_INTERRUPT'
            )
            print(f"Successfully cancelled step: {step_id}")
            return True
        except Exception as e:
            print(f"Error cancelling step: {e}")
            return False


def print_clusters(clusters: list):
    """Pretty print cluster list."""
    print("\n" + "="*80)
    print("ACTIVE EMR CLUSTERS")
    print("="*80)

    if not clusters:
        print("No active clusters found.")
        return

    for i, cluster in enumerate(clusters, 1):
        created = cluster['created']
        if isinstance(created, datetime):
            created = created.strftime('%Y-%m-%d %H:%M:%S')

        print(f"\n[{i}] Cluster ID: {cluster['id']}")
        print(f"    Name:      {cluster['name']}")
        print(f"    State:     {cluster['state']}")
        print(f"    Created:   {created}")


def print_yarn_apps(applications: list, cluster_name: str):
    """Pretty print YARN applications."""
    print(f"\n" + "-"*80)
    print(f"YARN APPLICATIONS on {cluster_name}")
    print("-"*80)

    if not applications:
        print("No running YARN applications found.")
        return

    for i, app in enumerate(applications, 1):
        print(f"\n[{i}] Application ID: {app['id']}")
        print(f"    Name:     {app['name']}")
        print(f"    Type:     {app.get('type', 'N/A')}")
        print(f"    User:     {app.get('user', 'N/A')}")
        print(f"    Queue:    {app.get('queue', 'N/A')}")
        print(f"    State:    {app['state']}")
        print(f"    Progress: {app.get('progress', 'N/A')}")


def interactive_mode():
    """Run the interactive CLI."""
    print("\n" + "="*80)
    print("EMR YARN JOB MANAGER")
    print("="*80)

    # Get AWS profile
    profile = input("\nEnter AWS profile name (press Enter for default): ").strip() or None

    # Get AWS region
    region = input("Enter AWS region (press Enter for default): ").strip() or None

    manager = EMRYarnManager(region=region, profile=profile)
    print(f"\nUsing profile: {manager.profile or 'default'}")
    print(f"Using region: {manager.region}")

    # Verify credentials
    try:
        sts = manager.session.client('sts')
        identity = sts.get_caller_identity()
        print(f"AWS Account: {identity['Account']}")
        print(f"User ARN: {identity['Arn']}")
    except Exception as e:
        print(f"Warning: Could not verify AWS credentials: {e}")
        print("Make sure you have valid AWS credentials configured.")

    # Get SSH key file path (optional)
    key_file = input("\nEnter path to SSH key file (press Enter to skip SSH features): ").strip() or None

    while True:
        # Fetch and display active clusters
        print("\nFetching active clusters...")
        clusters = manager.get_active_clusters()
        print_clusters(clusters)

        if not clusters:
            print("\nNo active clusters to manage. Exiting.")
            break

        # Select a cluster
        print("\n" + "-"*40)
        cluster_input = input("Enter cluster number to view YARN jobs (or 'q' to quit): ").strip()

        if cluster_input.lower() == 'q':
            break

        try:
            cluster_idx = int(cluster_input) - 1
            if cluster_idx < 0 or cluster_idx >= len(clusters):
                print("Invalid cluster number.")
                continue
        except ValueError:
            print("Please enter a valid number.")
            continue

        selected_cluster = clusters[cluster_idx]
        cluster_details = manager.get_cluster_details(selected_cluster['id'])

        print(f"\nCluster: {cluster_details.get('name', 'N/A')}")
        print(f"Master DNS: {cluster_details.get('master_dns', 'N/A')}")
        print(f"Master Instance ID: {cluster_details.get('master_instance_id', 'N/A')}")
        print(f"Release: {cluster_details.get('release_label', 'N/A')}")
        print(f"Applications: {', '.join(cluster_details.get('applications', []))}")

        # Fetch YARN applications
        print("\nFetching YARN applications...")

        # Try different methods to get YARN apps
        yarn_apps = []
        master_dns = cluster_details.get('master_dns')
        master_instance_id = cluster_details.get('master_instance_id')

        # Method 1: Try SSM first (most reliable, no network/SSH requirements)
        if master_instance_id:
            print("\n[Method 1] Trying AWS SSM (no SSH key needed)...")
            yarn_apps = manager.get_yarn_applications_via_ssm(master_instance_id)

        # Method 2: Try REST API (fast if network accessible)
        if not yarn_apps and master_dns and master_dns != 'N/A':
            print("\n[Method 2] Trying YARN ResourceManager REST API...")
            yarn_apps = manager.get_yarn_applications_via_rest(master_dns)

        # Method 3: Try SSH if key file provided
        if not yarn_apps and key_file and master_dns and master_dns != 'N/A':
            print("\n[Method 3] Trying SSH method...")
            yarn_apps = manager.get_yarn_applications_via_ssh(master_dns, key_file)

        # Method 4: Fall back to EMR steps
        if not yarn_apps:
            print("\n[Method 4] Trying EMR API for steps...")
            yarn_apps = manager.get_yarn_applications_via_api(selected_cluster['id'])

        print_yarn_apps(yarn_apps, cluster_details.get('name', selected_cluster['id']))

        if not yarn_apps:
            input("\nPress Enter to continue...")
            continue

        # Kill selected application
        print("\n" + "-"*40)
        app_input = input("Enter application number to KILL (or 'b' to go back): ").strip()

        if app_input.lower() == 'b':
            continue

        try:
            app_idx = int(app_input) - 1
            if app_idx < 0 or app_idx >= len(yarn_apps):
                print("Invalid application number.")
                continue
        except ValueError:
            print("Please enter a valid number.")
            continue

        selected_app = yarn_apps[app_idx]

        # Confirm kill
        print(f"\n⚠️  WARNING: You are about to kill:")
        print(f"    Application: {selected_app['id']}")
        print(f"    Name: {selected_app['name']}")

        confirm = input("\nType 'YES' to confirm: ").strip()

        if confirm == 'YES':
            # Determine kill method based on app type and available access
            master_instance_id = cluster_details.get('master_instance_id')

            if selected_app.get('type') == 'EMR Step':
                success = manager.cancel_emr_step(selected_cluster['id'], selected_app['id'])
            elif master_instance_id:
                # Prefer SSM method (no SSH key needed)
                print("Using SSM to kill application...")
                success = manager.kill_yarn_application_via_ssm(master_instance_id, selected_app['id'])
            elif key_file and master_dns:
                print("Using SSH to kill application...")
                success = manager.kill_yarn_application_via_ssh(master_dns, selected_app['id'], key_file)
            else:
                print("Cannot kill YARN application: no SSM access and no SSH key provided.")
                success = False

            if success:
                print("✓ Application killed successfully!")
            else:
                print("✗ Failed to kill application.")
        else:
            print("Kill cancelled.")

        input("\nPress Enter to continue...")

    print("\nGoodbye!")


def main():
    """Main entry point."""
    # Check for boto3
    try:
        import boto3
    except ImportError:
        print("Error: boto3 is required. Install it with: pip install boto3")
        sys.exit(1)

    interactive_mode()


if __name__ == "__main__":
    main()
