**AWS EMR CLI commands require the cluster ID, not the cluster name**.You can easily get the
cluster ID from the cluster name using a simple lookup. 
> Note that **Cluster names are not unique** - multiple clusters can have the same name

## Option 1: Get Cluster ID from Name

```bash
#!/bin/bash
CLUSTER_NAME="my-emr-cluster"  # Replace with your cluster name

echo "Looking up cluster ID for: $CLUSTER_NAME"
CLUSTER_ID=$(aws emr list-clusters --active --query "Clusters[?Name=='$CLUSTER_NAME'].Id" --output text)

if [ -z "$CLUSTER_ID" ]; then
    echo "Error: Cluster '$CLUSTER_NAME' not found or not active"
    exit 1
fi

echo "EMR Cluster Details:"
echo "==================="
echo "Cluster Name: $CLUSTER_NAME"
echo "Cluster ID: $CLUSTER_ID"

MASTER_IP=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].PrivateIpAddress' --output text)
MASTER_DNS=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].PrivateDnsName' --output text)

echo "Master Private IP: $MASTER_IP"
echo "Master Private DNS: $MASTER_DNS"
```

## Option 2: Function-Based Approach

```bash
#!/bin/bash

get_cluster_id() {
    local cluster_name=$1
    aws emr list-clusters --active --query "Clusters[?Name=='$cluster_name'].Id" --output text
}

# Usage
CLUSTER_NAME="my-emr-cluster"
CLUSTER_ID=$(get_cluster_id "$CLUSTER_NAME")

if [ -z "$CLUSTER_ID" ]; then
    echo "Cluster '$CLUSTER_NAME' not found"
    exit 1
fi

echo "Found cluster: $CLUSTER_NAME ($CLUSTER_ID)"
# ... rest of your script
```

## For EMR Steps with Cluster Name

```bash
#!/bin/bash
CLUSTER_NAME="my-emr-cluster"

# Get cluster ID
CLUSTER_ID=$(aws emr list-clusters --active --query "Clusters[?Name=='$CLUSTER_NAME'].Id" --output text)

# Submit step
aws emr add-steps --cluster-id $CLUSTER_ID --steps '[{
  "Name": "My Spark Job",
  "Type": "Spark",
  "ActionOnFailure": "CONTINUE",
  "Args": ["s3://your-bucket/myjob.py"]
}]'
```

## Important Notes:

- **Cluster names are not unique** - multiple clusters can have the same name
- If multiple active clusters have the same name, this will return multiple IDs
- Consider using `--cluster-states WAITING RUNNING` instead of `--active` for more control
- You might want to add error handling for cases where no cluster or multiple clusters are found
