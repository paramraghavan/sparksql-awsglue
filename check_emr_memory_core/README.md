# EMR Cluster Resources Script

A Python script to retrieve EMR cluster resource information including maximum and allocated memory and vCPUs.

## Requirements

```bash
pip install boto3
```

## Usage

### Basic Usage with AWS Profile

```bash
python emr_cluster_resources.py --cluster-id j-XXXXXXXXXXXXX --profile your-aws-profile
```

### Using Default AWS Credentials

```bash
python emr_cluster_resources.py --cluster-id j-XXXXXXXXXXXXX
```

### JSON Output

```bash
python emr_cluster_resources.py --cluster-id j-XXXXXXXXXXXXX --profile your-aws-profile --json
```

## Example Output

```
Cluster Name: My EMR Cluster
Cluster ID: j-XXXXXXXXXXXXX
Status: RUNNING
------------------------------------------------------------

MASTER Nodes:
  Instance Type: m5.xlarge
  Memory per Instance: 16 GB
  vCPUs per Instance: 4
  Requested Instances: 1
  Running Instances: 1
  Max Memory: 16 GB
  Max vCPUs: 4
  Allocated Memory: 16 GB
  Allocated vCPUs: 4

CORE Nodes:
  Instance Type: m5.2xlarge
  Memory per Instance: 32 GB
  vCPUs per Instance: 8
  Requested Instances: 3
  Running Instances: 3
  Max Memory: 96 GB
  Max vCPUs: 24
  Allocated Memory: 96 GB
  Allocated vCPUs: 24

============================================================
CLUSTER TOTALS:
  Maximum Memory: 112 GB
  Maximum vCPUs: 28
  Allocated Memory: 112 GB
  Allocated vCPUs: 28
============================================================
```

## AWS Permissions Required

The AWS profile/credentials need the following permissions:
- `emr:DescribeCluster`
- `emr:ListInstanceGroups`
- `ec2:DescribeInstanceTypes`

## Features

- Retrieves cluster resource information from AWS EMR
- Calculates total memory and vCPUs for the cluster
- Shows both maximum (requested) and allocated (running) resources
- Supports custom AWS profiles
- Outputs in human-readable or JSON format
- Automatically queries EC2 for unknown instance type specifications

## Notes

- The script includes specifications for common instance types (m5, r5, c5 families)
- For instance types not in the hardcoded list, it queries EC2 API automatically
- Memory is reported in GB, vCPUs as integer counts
