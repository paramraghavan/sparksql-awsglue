#!/usr/bin/env python3
"""
EMR Cluster Resource Information Script
Gets maximum and allocated memory/cores for an EMR cluster
"""

import boto3
import argparse
import json
from typing import Dict, Any


# Instance type specifications (memory in GB, vCPUs)
# Add more instance types as needed
INSTANCE_SPECS = {
    'm5.xlarge': {'memory': 16, 'vcpus': 4},
    'm5.2xlarge': {'memory': 32, 'vcpus': 8},
    'm5.4xlarge': {'memory': 64, 'vcpus': 16},
    'm5.8xlarge': {'memory': 128, 'vcpus': 32},
    'm5.12xlarge': {'memory': 192, 'vcpus': 48},
    'm5.16xlarge': {'memory': 256, 'vcpus': 64},
    'm5.24xlarge': {'memory': 384, 'vcpus': 96},
    'r5.xlarge': {'memory': 32, 'vcpus': 4},
    'r5.2xlarge': {'memory': 64, 'vcpus': 8},
    'r5.4xlarge': {'memory': 128, 'vcpus': 16},
    'r5.8xlarge': {'memory': 256, 'vcpus': 32},
    'r5.12xlarge': {'memory': 384, 'vcpus': 48},
    'r5.16xlarge': {'memory': 512, 'vcpus': 64},
    'r5.24xlarge': {'memory': 768, 'vcpus': 96},
    'c5.xlarge': {'memory': 8, 'vcpus': 4},
    'c5.2xlarge': {'memory': 16, 'vcpus': 8},
    'c5.4xlarge': {'memory': 32, 'vcpus': 16},
    'c5.9xlarge': {'memory': 72, 'vcpus': 36},
    'c5.12xlarge': {'memory': 96, 'vcpus': 48},
    'c5.18xlarge': {'memory': 144, 'vcpus': 72},
    'c5.24xlarge': {'memory': 192, 'vcpus': 96},
}


def get_instance_specs(instance_type: str, ec2_client) -> Dict[str, int]:
    """
    Get instance specifications (memory and vCPUs)
    First checks hardcoded specs, then queries EC2 if not found
    """
    if instance_type in INSTANCE_SPECS:
        return INSTANCE_SPECS[instance_type]
    
    # Query EC2 for instance type details
    try:
        response = ec2_client.describe_instance_types(InstanceTypes=[instance_type])
        if response['InstanceTypes']:
            instance_info = response['InstanceTypes'][0]
            return {
                'memory': instance_info['MemoryInfo']['SizeInMiB'] / 1024,  # Convert to GB
                'vcpus': instance_info['VCpuInfo']['DefaultVCpus']
            }
    except Exception as e:
        print(f"Warning: Could not fetch specs for {instance_type}: {e}")
    
    return {'memory': 0, 'vcpus': 0}


def get_cluster_resources(cluster_id: str, profile_name: str = None) -> Dict[str, Any]:
    """
    Get EMR cluster resource information
    
    Args:
        cluster_id: EMR cluster ID
        profile_name: AWS profile name (optional)
    
    Returns:
        Dictionary with cluster resource information
    """
    # Create boto3 session with profile
    if profile_name:
        session = boto3.Session(profile_name=profile_name)
    else:
        session = boto3.Session()
    
    emr_client = session.client('emr')
    ec2_client = session.client('ec2')
    
    # Get cluster details
    cluster = emr_client.describe_cluster(ClusterId=cluster_id)
    cluster_info = cluster['Cluster']
    
    print(f"Cluster Name: {cluster_info['Name']}")
    print(f"Cluster ID: {cluster_id}")
    print(f"Status: {cluster_info['Status']['State']}")
    print("-" * 60)
    
    total_memory = 0
    total_vcpus = 0
    
    # Get instance groups or instance fleets
    instance_groups = emr_client.list_instance_groups(ClusterId=cluster_id)
    
    results = {
        'cluster_id': cluster_id,
        'cluster_name': cluster_info['Name'],
        'status': cluster_info['Status']['State'],
        'node_groups': [],
        'total_max_memory_gb': 0,
        'total_max_vcpus': 0,
        'total_allocated_memory_gb': 0,
        'total_allocated_vcpus': 0
    }
    
    for group in instance_groups['InstanceGroups']:
        instance_type = group['InstanceType']
        requested_count = group['RequestedInstanceCount']
        running_count = group['RunningInstanceCount']
        group_type = group['InstanceGroupType']
        
        specs = get_instance_specs(instance_type, ec2_client)
        
        max_memory = specs['memory'] * requested_count
        max_vcpus = specs['vcpus'] * requested_count
        allocated_memory = specs['memory'] * running_count
        allocated_vcpus = specs['vcpus'] * running_count
        
        node_info = {
            'group_type': group_type,
            'instance_type': instance_type,
            'requested_instances': requested_count,
            'running_instances': running_count,
            'memory_per_instance_gb': specs['memory'],
            'vcpus_per_instance': specs['vcpus'],
            'max_memory_gb': max_memory,
            'max_vcpus': max_vcpus,
            'allocated_memory_gb': allocated_memory,
            'allocated_vcpus': allocated_vcpus
        }
        
        results['node_groups'].append(node_info)
        results['total_max_memory_gb'] += max_memory
        results['total_max_vcpus'] += max_vcpus
        results['total_allocated_memory_gb'] += allocated_memory
        results['total_allocated_vcpus'] += allocated_vcpus
        
        print(f"\n{group_type.upper()} Nodes:")
        print(f"  Instance Type: {instance_type}")
        print(f"  Memory per Instance: {specs['memory']} GB")
        print(f"  vCPUs per Instance: {specs['vcpus']}")
        print(f"  Requested Instances: {requested_count}")
        print(f"  Running Instances: {running_count}")
        print(f"  Max Memory: {max_memory} GB")
        print(f"  Max vCPUs: {max_vcpus}")
        print(f"  Allocated Memory: {allocated_memory} GB")
        print(f"  Allocated vCPUs: {allocated_vcpus}")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Get EMR cluster resource information (memory and cores)'
    )
    parser.add_argument(
        '--cluster-id',
        required=True,
        help='EMR cluster ID (e.g., j-XXXXXXXXXXXXX)'
    )
    parser.add_argument(
        '--profile',
        help='AWS profile name to use',
        default=None
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output in JSON format'
    )
    
    args = parser.parse_args()
    
    try:
        results = get_cluster_resources(args.cluster_id, args.profile)
        
        if args.json:
            print("\n" + "=" * 60)
            print(json.dumps(results, indent=2))
        else:
            print("\n" + "=" * 60)
            print("CLUSTER TOTALS:")
            print(f"  Maximum Memory: {results['total_max_memory_gb']} GB")
            print(f"  Maximum vCPUs: {results['total_max_vcpus']}")
            print(f"  Allocated Memory: {results['total_allocated_memory_gb']} GB")
            print(f"  Allocated vCPUs: {results['total_allocated_vcpus']}")
            print("=" * 60)
            
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
