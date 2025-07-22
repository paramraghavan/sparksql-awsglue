import boto3
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional
import pandas as pd


class EMRJobCostCalculator:
    def __init__(self, cluster_id: str, region: str = 'us-east-1'):
        """
        Initialize the EMR Job Cost Calculator

        Args:
            cluster_id: EMR cluster ID
            region: AWS region
        """
        self.cluster_id = cluster_id
        self.region = region

        # Initialize AWS clients
        self.emr_client = boto3.client('emr', region_name=region)
        self.ec2_client = boto3.client('ec2', region_name=region)

        # EMR pricing per hour (adjust based on your region)
        # These are example prices - update with actual pricing for your region
        self.emr_pricing = {
            'm5.xlarge': 0.27,  # Master node typical instance
            'm5.2xlarge': 0.54,  # Core nodes typical instance
            'm5.large': 0.135,  # Task nodes typical instance
            'c5.xlarge': 0.24,  # Alternative task node type
        }

        # Spot instance discount (typically 50-90% off on-demand)
        self.spot_discount = 0.7  # 70% discount

        # EMR Serverless pricing (per vCPU-hour and GB-hour)
        # Update these with actual pricing for your region
        self.emr_serverless_pricing = {
            'vcpu_hour': 0.052624,  # Price per vCPU hour
            'gb_hour': 0.0057785,  # Price per GB hour
        }

    def get_cluster_info(self) -> Dict:
        """Get cluster configuration and instance details"""
        try:
            cluster = self.emr_client.describe_cluster(ClusterId=self.cluster_id)
            instance_groups = self.emr_client.list_instance_groups(ClusterId=self.cluster_id)

            return {
                'cluster': cluster['Cluster'],
                'instance_groups': instance_groups['InstanceGroups']
            }
        except Exception as e:
            print(f"Error getting cluster info: {e}")
            return {}

    def get_spark_applications(self, job_name_filter: Optional[str] = None) -> List[Dict]:
        """Get completed Spark applications from EMR cluster"""
        try:
            # Get Spark History Server applications
            # Note: This requires Spark History Server to be enabled on the cluster
            applications = []

            # Alternative approach using EMR Steps if Spark jobs are submitted as steps
            steps = self.emr_client.list_steps(
                ClusterId=self.cluster_id,
                StepStates=['COMPLETED']
            )

            for step in steps['Steps']:
                if job_name_filter and job_name_filter.lower() not in step['Name'].lower():
                    continue

                app_info = {
                    'id': step['Id'],
                    'name': step['Name'],
                    'start_time': step['Status']['Timeline']['StartDateTime'],
                    'end_time': step['Status']['Timeline']['EndDateTime'],
                    'duration_seconds': (
                            step['Status']['Timeline']['EndDateTime'] -
                            step['Status']['Timeline']['StartDateTime']
                    ).total_seconds()
                }
                applications.append(app_info)

            return applications

        except Exception as e:
            print(f"Error getting Spark applications: {e}")
            return []

    def get_instance_pricing(self, instance_type: str, is_spot: bool = False) -> float:
        """Get hourly pricing for instance type"""
        base_price = self.emr_pricing.get(instance_type, 0.20)  # Default fallback price

        if is_spot:
            return base_price * self.spot_discount
        return base_price

    def calculate_job_cost(self, job_info: Dict, cluster_config: Dict) -> Dict:
        """Calculate cost for a specific job"""
        duration_hours = job_info['duration_seconds'] / 3600

        total_cost = 0.0
        cost_breakdown = {}

        # Calculate cost for each instance group
        for instance_group in cluster_config['instance_groups']:
            group_type = instance_group['InstanceGroupType']
            instance_type = instance_group['InstanceType']
            instance_count = instance_group['RequestedInstanceCount']

            # Determine if instances are spot or on-demand
            is_spot = False
            if group_type == 'TASK':
                # Assuming task nodes are mix of spot and on-demand
                # You'll need to adjust this logic based on your actual configuration
                spot_count = int(instance_count * 0.6)  # 60% spot instances
                ondemand_count = instance_count - spot_count

                # Spot instances cost
                spot_cost = (
                        spot_count *
                        self.get_instance_pricing(instance_type, is_spot=True) *
                        duration_hours
                )

                # On-demand instances cost
                ondemand_cost = (
                        ondemand_count *
                        self.get_instance_pricing(instance_type, is_spot=False) *
                        duration_hours
                )

                group_cost = spot_cost + ondemand_cost
                cost_breakdown[f'{group_type}_spot'] = spot_cost
                cost_breakdown[f'{group_type}_ondemand'] = ondemand_cost

            else:
                # Master and Core nodes are on-demand
                group_cost = (
                        instance_count *
                        self.get_instance_pricing(instance_type, is_spot=False) *
                        duration_hours
                )
                cost_breakdown[group_type] = group_cost

            total_cost += group_cost

        return {
            'job_id': job_info['id'],
            'job_name': job_info['name'],
            'duration_hours': duration_hours,
            'total_cost': total_cost,
            'cost_breakdown': cost_breakdown,
            'start_time': job_info['start_time'],
            'end_time': job_info['end_time']
        }

    def calculate_emr_serverless_cost(self, job_info: Dict, vcpu_count: int = 4, memory_gb: int = 16) -> Dict:
        """
        Calculate what the same job would cost on EMR Serverless

        Args:
            job_info: Job information with duration
            vcpu_count: Number of vCPUs to use for EMR Serverless
            memory_gb: Amount of memory in GB for EMR Serverless
        """
        duration_hours = job_info['duration_seconds'] / 3600

        vcpu_cost = vcpu_count * self.emr_serverless_pricing['vcpu_hour'] * duration_hours
        memory_cost = memory_gb * self.emr_serverless_pricing['gb_hour'] * duration_hours

        total_serverless_cost = vcpu_cost + memory_cost

        return {
            'job_id': job_info['id'],
            'job_name': job_info['name'],
            'duration_hours': duration_hours,
            'vcpu_count': vcpu_count,
            'memory_gb': memory_gb,
            'vcpu_cost': vcpu_cost,
            'memory_cost': memory_cost,
            'total_serverless_cost': total_serverless_cost
        }

    def compare_emr_vs_serverless(self, job_name_filter: Optional[str] = None,
                                  serverless_vcpu: int = 4, serverless_memory: int = 16) -> pd.DataFrame:
        """Generate comparison report between EMR and EMR Serverless costs"""
        print("Getting cluster information...")
        cluster_config = self.get_cluster_info()

        if not cluster_config:
            print("Failed to get cluster configuration")
            return pd.DataFrame()

        print("Getting completed Spark applications...")
        applications = self.get_spark_applications(job_name_filter)

        if not applications:
            print("No completed applications found")
            return pd.DataFrame()

        print(f"Calculating cost comparison for {len(applications)} jobs...")
        comparison_results = []

        for app in applications:
            try:
                # Calculate EMR cost
                emr_cost_info = self.calculate_job_cost(app, cluster_config)

                # Calculate EMR Serverless cost
                serverless_cost_info = self.calculate_emr_serverless_cost(
                    app, serverless_vcpu, serverless_memory
                )

                # Create comparison record
                comparison = {
                    'job_name': app['name'],
                    'duration_hours': emr_cost_info['duration_hours'],
                    'emr_total_cost': emr_cost_info['total_cost'],
                    'serverless_total_cost': serverless_cost_info['total_serverless_cost'],
                    'cost_difference': emr_cost_info['total_cost'] - serverless_cost_info['total_serverless_cost'],
                    'savings_percentage': ((emr_cost_info['total_cost'] - serverless_cost_info[
                        'total_serverless_cost']) / emr_cost_info['total_cost']) * 100,
                    'serverless_vcpu': serverless_vcpu,
                    'serverless_memory_gb': serverless_memory,
                    'start_time': app['start_time'],
                    'end_time': app['end_time']
                }

                comparison_results.append(comparison)

                savings_msg = f"saves ${abs(comparison['cost_difference']):.2f}" if comparison[
                                                                                        'cost_difference'] > 0 else f"costs ${abs(comparison['cost_difference']):.2f} more"
                print(
                    f"Job: {app['name']} - EMR: ${emr_cost_info['total_cost']:.2f}, Serverless: ${serverless_cost_info['total_serverless_cost']:.2f} (Serverless {savings_msg})")

            except Exception as e:
                print(f"Error calculating comparison for job {app.get('name', 'Unknown')}: {e}")

        if comparison_results:
            return pd.DataFrame(comparison_results)
        else:
            return pd.DataFrame()
        """Generate cost report for completed jobs"""
        print("Getting cluster information...")
        cluster_config = self.get_cluster_info()

        if not cluster_config:
            print("Failed to get cluster configuration")
            return pd.DataFrame()

        print("Getting completed Spark applications...")
        applications = self.get_spark_applications(job_name_filter)

        if not applications:
            print("No completed applications found")
            return pd.DataFrame()

        print(f"Calculating costs for {len(applications)} jobs...")
        cost_results = []

        for app in applications:
            try:
                cost_info = self.calculate_job_cost(app, cluster_config)
                cost_results.append(cost_info)
                print(f"Processed job: {cost_info['job_name']} - Cost: ${cost_info['total_cost']:.2f}")
            except Exception as e:
                print(f"Error calculating cost for job {app.get('name', 'Unknown')}: {e}")

        # Create DataFrame
        if cost_results:
            df = pd.DataFrame(cost_results)
            return df
        else:
            return pd.DataFrame()

    def generate_cost_report(self, job_name_filter: Optional[str] = None) -> pd.DataFrame:
        """Export cost report to CSV"""
        if not df.empty:
            df.to_csv(filename, index=False)
            print(f"Report exported to {filename}")
        else:
            print("No data to export")


# Usage example
def main():
    # Configuration
    CLUSTER_ID = "j-XXXXXXXXXXXXX"  # Replace with your EMR cluster ID
    REGION = "us-east-1"  # Replace with your AWS region
    JOB_NAME_FILTER = "my-spark-job"  # Optional: filter by job name

    # Initialize calculator
    calculator = EMRJobCostCalculator(CLUSTER_ID, REGION)

    # Generate cost report
    cost_df = calculator.generate_cost_report(job_name_filter=JOB_NAME_FILTER)

    if not cost_df.empty:
        # Display summary
        print("\n" + "=" * 50)
        print("COST SUMMARY")
        print("=" * 50)
        print(f"Total jobs analyzed: {len(cost_df)}")
        print(f"Total cost: ${cost_df['total_cost'].sum():.2f}")
        print(f"Average cost per job: ${cost_df['total_cost'].mean():.2f}")
        print(f"Average duration: {cost_df['duration_hours'].mean():.2f} hours")

        # Show top 5 most expensive jobs
        print("\nTop 5 Most Expensive Jobs:")
        top_jobs = cost_df.nlargest(5, 'total_cost')[['job_name', 'total_cost', 'duration_hours']]
        print(top_jobs.to_string(index=False))

        # Export to CSV
        calculator.export_report(cost_df)

        # Display detailed breakdown for first job (example)
        if len(cost_df) > 0:
            print(f"\nDetailed cost breakdown for '{cost_df.iloc[0]['job_name']}':")
            breakdown = cost_df.iloc[0]['cost_breakdown']
            for component, cost in breakdown.items():
                print(f"  {component}: ${cost:.2f}")

    else:
        print("No jobs found matching the criteria")


if __name__ == "__main__":
    main()