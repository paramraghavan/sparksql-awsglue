# Auto-Populate Script

Save this as `generate_recreate_script.sh`:

```bash
#!/bin/bash

#==============================================================================
# EMR Cluster Configuration Extractor and Script Generator
# This script extracts configuration from an existing EMR cluster and 
# generates a ready-to-use recreation script
#==============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

#------------------------------------------------------------------------------
# Check parameters
#------------------------------------------------------------------------------

CLUSTER_ID=$1
OUTPUT_SCRIPT="recreate_emr_cluster.sh"

if [ -z "$CLUSTER_ID" ]; then
    echo -e "${RED}Usage: ./generate_recreate_script.sh j-XXXXXXXXXXXXX [output_file]${NC}"
    echo ""
    echo "Example:"
    echo "  ./generate_recreate_script.sh j-2AXXXXXXXXXX"
    echo "  ./generate_recreate_script.sh j-2AXXXXXXXXXX my_custom_script.sh"
    exit 1
fi

if [ ! -z "$2" ]; then
    OUTPUT_SCRIPT=$2
fi

echo -e "${GREEN}=========================================="
echo "EMR Configuration Extractor"
echo -e "==========================================${NC}"
echo ""
echo "Cluster ID: $CLUSTER_ID"
echo "Output Script: $OUTPUT_SCRIPT"
echo ""

#------------------------------------------------------------------------------
# Verify cluster exists and is accessible
#------------------------------------------------------------------------------

echo "Checking cluster access..."
if ! aws emr describe-cluster --cluster-id $CLUSTER_ID &>/dev/null; then
    echo -e "${RED}ERROR: Cannot access cluster $CLUSTER_ID${NC}"
    echo "Please check:"
    echo "  - Cluster ID is correct"
    echo "  - AWS credentials are configured"
    echo "  - You have permission to access this cluster"
    exit 1
fi

CLUSTER_STATE=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Status.State' --output text)
echo -e "Cluster State: ${YELLOW}$CLUSTER_STATE${NC}"
echo ""

#------------------------------------------------------------------------------
# Extract Basic Configuration
#------------------------------------------------------------------------------

echo "Extracting cluster configuration..."

CLUSTER_NAME=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Name' --output text)
RELEASE_LABEL=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.ReleaseLabel' --output text)
AWS_REGION=$(aws configure get region)
if [ -z "$AWS_REGION" ]; then
    AWS_REGION="us-east-1"
fi

# Networking
SUBNET_ID=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Ec2InstanceAttributes.Ec2SubnetId' --output text)
KEY_NAME=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Ec2InstanceAttributes.Ec2KeyName' --output text)

# IAM Roles
SERVICE_ROLE=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.ServiceRole' --output text)
EC2_ROLE=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Ec2InstanceAttributes.IamInstanceProfile' --output text)
AUTOSCALING_ROLE=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.AutoScalingRole' --output text)
if [ "$AUTOSCALING_ROLE" == "None" ] || [ -z "$AUTOSCALING_ROLE" ]; then
    AUTOSCALING_ROLE="EMR_AutoScaling_DefaultRole"
fi

# S3 Paths
LOG_URI=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.LogUri' --output text)

# Security Configuration
SECURITY_CONFIG=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.SecurityConfiguration' --output text)
if [ "$SECURITY_CONFIG" == "None" ]; then
    SECURITY_CONFIG=""
fi

# Security Groups
MASTER_SG=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Ec2InstanceAttributes.EmrManagedMasterSecurityGroup' --output text)
SLAVE_SG=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Ec2InstanceAttributes.EmrManagedSlaveSecurityGroup' --output text)
SERVICE_SG=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Ec2InstanceAttributes.ServiceAccessSecurityGroup' --output text)

if [ "$MASTER_SG" == "None" ]; then MASTER_SG=""; fi
if [ "$SLAVE_SG" == "None" ]; then SLAVE_SG=""; fi
if [ "$SERVICE_SG" == "None" ]; then SERVICE_SG=""; fi

#------------------------------------------------------------------------------
# Extract Instance Configuration
#------------------------------------------------------------------------------

# Get all instance groups
INSTANCE_GROUPS_JSON=$(aws emr list-instance-groups --cluster-id $CLUSTER_ID)

# Master
MASTER_INSTANCE_TYPE=$(echo "$INSTANCE_GROUPS_JSON" | jq -r '.InstanceGroups[] | select(.InstanceGroupType=="MASTER") | .InstanceType')
MASTER_INSTANCE_COUNT=$(echo "$INSTANCE_GROUPS_JSON" | jq -r '.InstanceGroups[] | select(.InstanceGroupType=="MASTER") | .RequestedInstanceCount')

# Core
CORE_INSTANCE_TYPE=$(echo "$INSTANCE_GROUPS_JSON" | jq -r '.InstanceGroups[] | select(.InstanceGroupType=="CORE") | .InstanceType')
CORE_INSTANCE_COUNT=$(echo "$INSTANCE_GROUPS_JSON" | jq -r '.InstanceGroups[] | select(.InstanceGroupType=="CORE") | .RequestedInstanceCount')
CORE_EBS_SIZE=$(echo "$INSTANCE_GROUPS_JSON" | jq -r '.InstanceGroups[] | select(.InstanceGroupType=="CORE") | .EbsBlockDevices[0].VolumeSpecification.SizeInGB // 100')

# Task (optional)
TASK_INSTANCE_TYPE=$(echo "$INSTANCE_GROUPS_JSON" | jq -r '.InstanceGroups[] | select(.InstanceGroupType=="TASK") | .InstanceType // "m5.2xlarge"')
TASK_INSTANCE_COUNT=$(echo "$INSTANCE_GROUPS_JSON" | jq -r '.InstanceGroups[] | select(.InstanceGroupType=="TASK") | .RequestedInstanceCount // 0')
TASK_EBS_SIZE=$(echo "$INSTANCE_GROUPS_JSON" | jq -r '.InstanceGroups[] | select(.InstanceGroupType=="TASK") | .EbsBlockDevices[0].VolumeSpecification.SizeInGB // 100')

if [ "$TASK_INSTANCE_COUNT" == "null" ] || [ -z "$TASK_INSTANCE_COUNT" ]; then
    TASK_INSTANCE_COUNT=0
fi

#------------------------------------------------------------------------------
# Extract Applications
#------------------------------------------------------------------------------

APPLICATIONS_ARRAY=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Applications[*].Name' --output text)
APPLICATIONS=""
for app in $APPLICATIONS_ARRAY; do
    APPLICATIONS="$APPLICATIONS Name=$app"
done
APPLICATIONS=$(echo $APPLICATIONS | xargs)  # Trim whitespace

#------------------------------------------------------------------------------
# Extract Tags
#------------------------------------------------------------------------------

TAGS_JSON=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Tags')
TAGS=""
if [ "$TAGS_JSON" != "null" ] && [ "$TAGS_JSON" != "[]" ]; then
    TAGS=$(echo "$TAGS_JSON" | jq -r '.[] | "Key=" + .Key + ",Value=" + .Value' | paste -sd ' ')
fi

if [ -z "$TAGS" ]; then
    TAGS="Key=Environment,Value=Production Key=ManagedBy,Value=EMRRecreationScript"
fi

#------------------------------------------------------------------------------
# Extract Bootstrap Actions
#------------------------------------------------------------------------------

BOOTSTRAP_SCRIPT=$(aws emr list-bootstrap-actions --cluster-id $CLUSTER_ID --query 'BootstrapActions[0].ScriptPath' --output text 2>/dev/null || echo "")
if [ "$BOOTSTRAP_SCRIPT" == "None" ] || [ -z "$BOOTSTRAP_SCRIPT" ]; then
    BOOTSTRAP_SCRIPT=""
fi

#------------------------------------------------------------------------------
# Extract and Save Configurations
#------------------------------------------------------------------------------

CONFIGURATIONS_FILE="cluster_configurations_${CLUSTER_ID}.json"
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Configurations' > $CONFIGURATIONS_FILE

# Check if configurations exist
if grep -q "null" $CONFIGURATIONS_FILE || [ ! -s $CONFIGURATIONS_FILE ]; then
    CONFIGURATIONS_FILE=""
    rm -f cluster_configurations_${CLUSTER_ID}.json
else
    echo -e "${GREEN}✓ Saved configurations to: $CONFIGURATIONS_FILE${NC}"
fi

#------------------------------------------------------------------------------
# Display Summary
#------------------------------------------------------------------------------

echo ""
echo -e "${GREEN}Configuration extracted successfully!${NC}"
echo ""
echo "Summary:"
echo "  Cluster Name:      $CLUSTER_NAME"
echo "  EMR Version:       $RELEASE_LABEL"
echo "  Region:            $AWS_REGION"
echo "  Subnet:            $SUBNET_ID"
echo "  Key Pair:          $KEY_NAME"
echo "  Master Instance:   $MASTER_INSTANCE_TYPE"
echo "  Core Instances:    $CORE_INSTANCE_COUNT x $CORE_INSTANCE_TYPE"
echo "  Task Instances:    $TASK_INSTANCE_COUNT x $TASK_INSTANCE_TYPE"
echo "  Applications:      $APPLICATIONS"
echo ""

#------------------------------------------------------------------------------
# Generate Recreation Script
#------------------------------------------------------------------------------

echo "Generating recreation script..."

cat > $OUTPUT_SCRIPT << 'SCRIPT_HEADER'
#!/bin/bash

#==============================================================================
# EMR Cluster Recreation Script
# Auto-generated from existing cluster configuration
#==============================================================================

set -e  # Exit on error

SCRIPT_HEADER

cat >> $OUTPUT_SCRIPT << SCRIPT_CONFIG
#------------------------------------------------------------------------------
# CONFIGURATION - Auto-extracted from cluster $CLUSTER_ID
#------------------------------------------------------------------------------

# Basic Cluster Info
CLUSTER_NAME="$CLUSTER_NAME-recreated"
RELEASE_LABEL="$RELEASE_LABEL"
AWS_REGION="$AWS_REGION"

# Networking
SUBNET_ID="$SUBNET_ID"
KEY_NAME="$KEY_NAME"

# IAM Roles
SERVICE_ROLE="$SERVICE_ROLE"
EC2_ROLE="$EC2_ROLE"
AUTOSCALING_ROLE="$AUTOSCALING_ROLE"

# S3 Paths
LOG_URI="$LOG_URI"
BOOTSTRAP_SCRIPT="$BOOTSTRAP_SCRIPT"

# Security Configuration
SECURITY_CONFIG="$SECURITY_CONFIG"

# Security Groups
MASTER_SG="$MASTER_SG"
SLAVE_SG="$SLAVE_SG"
SERVICE_SG="$SERVICE_SG"

#------------------------------------------------------------------------------
# INSTANCE CONFIGURATION
#------------------------------------------------------------------------------

# Master Node
MASTER_INSTANCE_TYPE="$MASTER_INSTANCE_TYPE"
MASTER_INSTANCE_COUNT=$MASTER_INSTANCE_COUNT

# Core Nodes
CORE_INSTANCE_TYPE="$CORE_INSTANCE_TYPE"
CORE_INSTANCE_COUNT=$CORE_INSTANCE_COUNT
CORE_EBS_SIZE=$CORE_EBS_SIZE

# Task Nodes
TASK_INSTANCE_TYPE="$TASK_INSTANCE_TYPE"
TASK_INSTANCE_COUNT=$TASK_INSTANCE_COUNT
TASK_EBS_SIZE=$TASK_EBS_SIZE

#------------------------------------------------------------------------------
# APPLICATIONS
#------------------------------------------------------------------------------

APPLICATIONS="$APPLICATIONS"

#------------------------------------------------------------------------------
# TAGS
#------------------------------------------------------------------------------

TAGS="$TAGS"

#------------------------------------------------------------------------------
# CONFIGURATIONS FILE
#------------------------------------------------------------------------------

CONFIGURATIONS_FILE="$CONFIGURATIONS_FILE"

SCRIPT_CONFIG

cat >> $OUTPUT_SCRIPT << 'SCRIPT_BODY'

#------------------------------------------------------------------------------
# SCRIPT START - DO NOT MODIFY BELOW THIS LINE
#------------------------------------------------------------------------------

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=========================================="
echo "EMR Cluster Recreation Script"
echo -e "==========================================${NC}"
echo ""
echo "Cluster Name: $CLUSTER_NAME"
echo "EMR Version:  $RELEASE_LABEL"
echo "Region:       $AWS_REGION"
echo ""

# Validate required parameters
if [ -z "$SUBNET_ID" ]; then
    echo -e "${RED}ERROR: SUBNET_ID is not set${NC}"
    exit 1
fi

if [ -z "$KEY_NAME" ]; then
    echo -e "${RED}ERROR: KEY_NAME is not set${NC}"
    exit 1
fi

if [ -z "$LOG_URI" ]; then
    echo -e "${RED}ERROR: LOG_URI is not set${NC}"
    exit 1
fi

#------------------------------------------------------------------------------
# Build EC2 Attributes
#------------------------------------------------------------------------------

EC2_ATTRIBUTES="KeyName=$KEY_NAME,SubnetId=$SUBNET_ID,InstanceProfile=$EC2_ROLE"

if [ ! -z "$MASTER_SG" ] && [ ! -z "$SLAVE_SG" ]; then
    EC2_ATTRIBUTES="$EC2_ATTRIBUTES,EmrManagedMasterSecurityGroup=$MASTER_SG,EmrManagedSlaveSecurityGroup=$SLAVE_SG"
    if [ ! -z "$SERVICE_SG" ]; then
        EC2_ATTRIBUTES="$EC2_ATTRIBUTES,ServiceAccessSecurityGroup=$SERVICE_SG"
    fi
fi

#------------------------------------------------------------------------------
# Build Instance Groups
#------------------------------------------------------------------------------

INSTANCE_GROUPS="[
  {
    \"InstanceGroupType\": \"MASTER\",
    \"InstanceType\": \"$MASTER_INSTANCE_TYPE\",
    \"InstanceCount\": $MASTER_INSTANCE_COUNT,
    \"Name\": \"Master\"
  },
  {
    \"InstanceGroupType\": \"CORE\",
    \"InstanceType\": \"$CORE_INSTANCE_TYPE\",
    \"InstanceCount\": $CORE_INSTANCE_COUNT,
    \"Name\": \"Core\",
    \"EbsConfiguration\": {
      \"EbsBlockDeviceConfigs\": [
        {
          \"VolumeSpecification\": {
            \"VolumeType\": \"gp3\",
            \"SizeInGB\": $CORE_EBS_SIZE
          },
          \"VolumesPerInstance\": 2
        }
      ]
    }
  }"

if [ $TASK_INSTANCE_COUNT -gt 0 ]; then
    INSTANCE_GROUPS="$INSTANCE_GROUPS,
  {
    \"InstanceGroupType\": \"TASK\",
    \"InstanceType\": \"$TASK_INSTANCE_TYPE\",
    \"InstanceCount\": $TASK_INSTANCE_COUNT,
    \"Name\": \"Task\",
    \"EbsConfiguration\": {
      \"EbsBlockDeviceConfigs\": [
        {
          \"VolumeSpecification\": {
            \"VolumeType\": \"gp3\",
            \"SizeInGB\": $TASK_EBS_SIZE
          },
          \"VolumesPerInstance\": 2
        }
      ]
    }
  }"
fi

INSTANCE_GROUPS="$INSTANCE_GROUPS
]"

#------------------------------------------------------------------------------
# Build AWS CLI Command
#------------------------------------------------------------------------------

CMD="aws emr create-cluster \
  --name \"$CLUSTER_NAME\" \
  --release-label $RELEASE_LABEL \
  --applications $APPLICATIONS \
  --region $AWS_REGION \
  --log-uri $LOG_URI \
  --service-role $SERVICE_ROLE \
  --ec2-attributes $EC2_ATTRIBUTES \
  --instance-groups '$INSTANCE_GROUPS' \
  --enable-debugging \
  --tags $TAGS"

# Add configurations if specified
if [ ! -z "$CONFIGURATIONS_FILE" ] && [ -f "$CONFIGURATIONS_FILE" ]; then
    CMD="$CMD --configurations file://$CONFIGURATIONS_FILE"
fi

# Add bootstrap actions if specified
if [ ! -z "$BOOTSTRAP_SCRIPT" ]; then
    CMD="$CMD --bootstrap-actions Path=$BOOTSTRAP_SCRIPT,Name=CustomBootstrap"
fi

# Add security configuration if specified
if [ ! -z "$SECURITY_CONFIG" ]; then
    CMD="$CMD --security-configuration $SECURITY_CONFIG"
fi

# Add auto-scaling role if using task nodes
if [ $TASK_INSTANCE_COUNT -gt 0 ]; then
    CMD="$CMD --auto-scaling-role $AUTOSCALING_ROLE"
fi

#------------------------------------------------------------------------------
# Execute Cluster Creation
#------------------------------------------------------------------------------

echo "Creating EMR cluster..."
echo ""
echo -e "${YELLOW}Command to execute:${NC}"
echo "$CMD"
echo ""
echo -e "${YELLOW}Proceed? (yes/no)${NC}"
read -r CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    echo "Cancelled."
    exit 0
fi

# Execute and capture cluster ID
echo ""
echo "Submitting cluster creation request..."
NEW_CLUSTER_ID=$(eval $CMD --output text)

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}=========================================="
    echo "SUCCESS!"
    echo -e "==========================================${NC}"
    echo "New Cluster ID: $NEW_CLUSTER_ID"
    echo ""
    echo "Monitoring cluster creation..."
    echo ""
    
    # Monitor cluster status
    for i in {1..60}; do
        STATUS=$(aws emr describe-cluster --cluster-id $NEW_CLUSTER_ID --region $AWS_REGION --query 'Cluster.Status.State' --output text 2>/dev/null)
        TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
        
        if [ "$STATUS" == "RUNNING" ]; then
            echo -e "[$TIMESTAMP] Status: ${GREEN}$STATUS${NC}"
            echo ""
            echo -e "${GREEN}=========================================="
            echo "Cluster is RUNNING!"
            echo -e "==========================================${NC}"
            
            # Get master DNS
            MASTER_DNS=$(aws emr describe-cluster --cluster-id $NEW_CLUSTER_ID --region $AWS_REGION --query 'Cluster.MasterPublicDnsName' --output text 2>/dev/null)
            
            echo ""
            echo "Cluster Details:"
            echo "  Cluster ID:  $NEW_CLUSTER_ID"
            echo "  Master DNS:  $MASTER_DNS"
            echo ""
            echo "Web Interfaces:"
            echo "  YARN ResourceManager: http://$MASTER_DNS:8088"
            echo "  Spark History Server: http://$MASTER_DNS:18080"
            echo "  Ganglia:              http://$MASTER_DNS/ganglia/"
            echo ""
            echo "SSH Command:"
            echo "  ssh -i /path/to/$KEY_NAME.pem hadoop@$MASTER_DNS"
            echo ""
            
            break
        elif [ "$STATUS" == "TERMINATED" ] || [ "$STATUS" == "TERMINATED_WITH_ERRORS" ]; then
            echo -e "[$TIMESTAMP] Status: ${RED}$STATUS${NC}"
            echo ""
            echo -e "${RED}ERROR: Cluster creation failed!${NC}"
            echo ""
            aws emr describe-cluster --cluster-id $NEW_CLUSTER_ID --region $AWS_REGION --query 'Cluster.Status.StateChangeReason'
            exit 1
        elif [ "$STATUS" == "STARTING" ] || [ "$STATUS" == "BOOTSTRAPPING" ]; then
            echo -e "[$TIMESTAMP] Status: ${YELLOW}$STATUS${NC}"
        else
            echo "[$TIMESTAMP] Status: $STATUS"
        fi
        
        sleep 30
    done
    
else
    echo ""
    echo -e "${RED}ERROR: Failed to create cluster${NC}"
    exit 1
fi
SCRIPT_BODY

chmod +x $OUTPUT_SCRIPT

echo ""
echo -e "${GREEN}=========================================="
echo "Script Generated Successfully!"
echo -e "==========================================${NC}"
echo ""
echo "Output file: $OUTPUT_SCRIPT"
if [ ! -z "$CONFIGURATIONS_FILE" ]; then
    echo "Config file: $CONFIGURATIONS_FILE"
fi
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Review the generated script: vi $OUTPUT_SCRIPT"
echo "  2. Make any needed adjustments"
echo "  3. Run: ./$OUTPUT_SCRIPT"
echo ""
echo -e "${YELLOW}Quick recreation (skip old cluster termination):${NC}"
echo "  ./$OUTPUT_SCRIPT"
echo ""
echo -e "${YELLOW}Full process (terminate old, create new):${NC}"
echo "  aws emr terminate-clusters --cluster-ids $CLUSTER_ID"
echo "  ./$OUTPUT_SCRIPT"
echo ""
```

---

## How to Use

**Step 1: Make the generator script executable**

```bash
chmod +x generate_recreate_script.sh
```

**Step 2: Run it with your cluster ID**

```bash
# Basic usage
./generate_recreate_script.sh j-XXXXXXXXXXXXX

# Or specify custom output filename
./generate_recreate_script.sh j-XXXXXXXXXXXXX my_cluster_recreate.sh
```

**Step 3: The script will:**

- Extract ALL configuration from your existing cluster
- Generate a ready-to-run recreation script
- Save any custom configurations to a JSON file
- Display a summary of what was extracted

**Step 4: Review and run the generated script**

```bash
# Review it
cat recreate_emr_cluster.sh

# Make any tweaks if needed
vi recreate_emr_cluster.sh

# Run it!
./recreate_emr_cluster.sh
```

---

## Complete Workflow Example

```bash
# 1. Generate recreation script from existing cluster
./generate_recreate_script.sh j-2AXXXXXXXXXX

# Output will show:
# ✓ Saved configurations to: cluster_configurations_j-2AXXXXXXXXXX.json
# ✓ Script generated: recreate_emr_cluster.sh

# 2. (Optional) Terminate old cluster
aws emr terminate-clusters --cluster-ids j-2AXXXXXXXXXX

# 3. Create new cluster immediately (don't wait for termination)
./recreate_emr_cluster.sh

# 4. Script will monitor and show when cluster is RUNNING
```

---

## What Gets Extracted

The script automatically extracts:

- ✅ Cluster name, EMR version, region
- ✅ Network settings (subnet, key pair)
- ✅ IAM roles (service, EC2, autoscaling)
- ✅ Instance types and counts (master, core, task)
- ✅ EBS configuration
- ✅ Security groups
- ✅ All installed applications
- ✅ Tags
- ✅ Custom Spark/YARN/Hadoop configurations
- ✅ Bootstrap actions
- ✅ Security configurations
- ✅ Logging URIs

---

## Troubleshooting

**If you get "jq: command not found":**

```bash
# Install jq
# Amazon Linux / RHEL / CentOS
sudo yum install jq -y

# Ubuntu / Debian
sudo apt-get install jq -y

# macOS
brew install jq
```

**If script fails to extract config:**

```bash
# Check AWS CLI is configured
aws sts get-caller-identity

# Check you can access the cluster
aws emr describe-cluster --cluster-id j-XXXXXXXXXXXXX

# Check your permissions
aws iam get-user
```

## Where shuoudl i run the above script

# Where to Run the Script

You should run this script from **any machine that has AWS CLI access and proper permissions**. Here are your options:

---

## Option 1: Your Edge Node (RECOMMENDED)

If your edge node already has AWS CLI configured, this is the **best place** to run it:

```bash
# On your edge node, check if AWS CLI is configured
aws sts get-caller-identity
aws emr list-clusters --active

# If those work, you're good! Run the script there:
cd ~
# Upload or create the generate_recreate_script.sh
./generate_recreate_script.sh j-XXXXXXXXXXXXX
```

**Advantages:**

- Already has network access to AWS
- Likely has AWS credentials configured
- Close to your EMR environment
- You can run the recreation script from the same place

---

## Option 2: Your Local Laptop/Desktop

If you have AWS CLI installed locally:

```bash
# Check if AWS CLI is installed
aws --version

# Check if configured
aws sts get-caller-identity

# If yes, run the script locally:
./generate_recreate_script.sh j-XXXXXXXXXXXXX
```

**Advantages:**

- Easy to edit scripts with your preferred editor
- Can version control the scripts
- No need to SSH anywhere

---

## Option 3: AWS CloudShell (EASIEST)

AWS CloudShell is a browser-based shell that comes with AWS CLI pre-configured:

**Steps:**

1. Go to AWS Console
2. Click the CloudShell icon (terminal icon in top navigation bar)
3. Wait for shell to load
4. Upload the script or paste it:

```bash
# In CloudShell
cat > generate_recreate_script.sh << 'EOF'
[paste the entire script here]
EOF

chmod +x generate_recreate_script.sh

# Install jq (already installed in CloudShell usually)
# Run the script
./generate_recreate_script.sh j-XXXXXXXXXXXXX

# Download generated files
# Click Actions > Download file
# Enter: recreate_emr_cluster.sh
```

**Advantages:**

- No setup needed
- AWS CLI pre-configured
- Browser-based
- Always up to date

---

## Option 4: Bastion/Jump Host

If you access AWS through a bastion host:

```bash
# SSH to bastion
ssh user@bastion-host

# Check AWS CLI
aws --version

# Run script there
./generate_recreate_script.sh j-XXXXXXXXXXXXX
```

---

## Quick Setup Guide for Edge Node

Since you mentioned working from an edge node, here's how to set it up there:

```bash
# 1. SSH to your edge node
ssh user@your-edge-node

# 2. Check if AWS CLI is installed
aws --version

# If not installed:
# Amazon Linux / RHEL / CentOS
sudo yum install awscli -y

# Ubuntu / Debian
sudo apt-get install awscli -y

# 3. Configure AWS CLI (if not already done)
aws configure
# Enter:
#   AWS Access Key ID: [your-key]
#   AWS Secret Access Key: [your-secret]
#   Default region: us-east-1 (or your region)
#   Default output format: json

# 4. Test access
aws emr list-clusters --active

# 5. Install jq (needed for the script)
sudo yum install jq -y
# OR
sudo apt-get install jq -y

# 6. Create the generator script
vi generate_recreate_script.sh
# Paste the script content, save and exit (:wq)

chmod +x generate_recreate_script.sh

# 7. Run it
./generate_recreate_script.sh j-XXXXXXXXXXXXX
```

---

## What You Need Wherever You Run It

**Required:**

- ✅ AWS CLI installed
- ✅ AWS credentials configured with permissions to:
    - `emr:DescribeCluster`
    - `emr:ListClusters`
    - `emr:ListInstanceGroups`
    - `emr:ListBootstrapActions`
    - `emr:CreateCluster` (for when you run the recreation script)
    - `emr:TerminateJobFlows` (if you're terminating the old cluster)
- ✅ `jq` command installed
- ✅ Bash shell

**To verify you have everything:**

```bash
# Check AWS CLI
aws --version

# Check jq
jq --version

# Check permissions
aws emr list-clusters --active

# Check you can describe your cluster
aws emr describe-cluster --cluster-id j-XXXXXXXXXXXXX
```

---

## My Recommendation

**Run it from your edge node** because:

1. AWS CLI is likely already configured there
2. You're already familiar with that environment
3. You can run the recreation script from the same place
4. No need to set up credentials elsewhere

**Quick test to see if your edge node is ready:**

```bash
# SSH to edge node
ssh user@your-edge-node

# One command test
aws emr list-clusters --active && jq --version && echo "✅ Ready to run the script!"
```

