## Bootstrap Script: install_dependencies.sh

When you run a PySpark job on Amazon EMR, you often need additional Python libraries beyond what's pre-installed in the
EMR environment. The `install_dependencies.sh` bootstrap script is a solution for this problem.

### What is a Bootstrap Script?

Bootstrap scripts are shell scripts that run when an EMR cluster is being created, before the applications (like Spark)
start running. They run on all nodes in the cluster (master and core/task nodes).

### How the install_dependencies.sh Script Works:

1. **Execution timing**: The script runs during cluster provisioning, before your Spark jobs start

2. **Content explanation**:
   ```bash
   #!/bin/bash
   sudo pip3 install -r /home/hadoop/requirements.txt
   ```
    - The first line `#!/bin/bash` (shebang) tells the system this is a bash script
    - The second line uses `pip3` with sudo privileges to install Python packages from your requirements.txt file

3. **Location of requirements.txt**: The script assumes your requirements.txt file will be in the `/home/hadoop/`
   directory. You'll need to make sure this file gets there, typically by:
    - Including it in your code package
    - Uploading it to S3 and having the bootstrap action download it first

### Creating and Using the Bootstrap Script:

1. **Create the script locally**:
   ```bash
   # Create the script file
   cat > install_dependencies.sh << 'EOF'
   #!/bin/bash
   
   # Download requirements.txt from S3 if not bundled with the script
   aws s3 cp s3://my-bucket/code/requirements.txt /home/hadoop/requirements.txt
   
   # Install the dependencies
   sudo pip3 install -r /home/hadoop/requirements.txt
   EOF
   
   # Make it executable
   chmod +x install_dependencies.sh
   ```

2. **Upload to S3**:
   ```bash
   aws s3 cp install_dependencies.sh s3://my-bucket/bootstrap/install_dependencies.sh
   ```

3. **Reference in EMR cluster creation**:
   ```bash
   aws emr create-cluster \
       --name "Customer Segmentation Cluster" \
       --release-label emr-6.10.0 \
       --applications Name=Spark \
       --bootstrap-actions Path="s3://my-bucket/bootstrap/install_dependencies.sh" \
       # Other cluster configuration options...
   ```

### Common Enhancements:

1. **Installing system packages**:
   ```bash
   #!/bin/bash
   # Install system dependencies if needed
   sudo yum install -y python3-devel
   
   # Install Python packages
   sudo pip3 install -r /home/hadoop/requirements.txt
   ```

2. **Configuration for different node types**:
   ```bash
   #!/bin/bash
   # Determine if this is the master node
   IS_MASTER=false
   if grep isMaster /mnt/var/lib/info/instance.json | grep true; then
     IS_MASTER=true
   fi
   
   # Run certain commands only on master
   if [ "$IS_MASTER" = true ]; then
     # Master-specific installations
     sudo pip3 install jupyter
   fi
   
   # Run on all nodes
   sudo pip3 install -r /home/hadoop/requirements.txt
   ```

3. **Error handling**:
   ```bash
   #!/bin/bash
   set -e  # Exit immediately if a command fails
   
   echo "Starting dependency installation..."
   
   # Download requirements
   aws s3 cp s3://my-bucket/code/requirements.txt /home/hadoop/requirements.txt || {
     echo "Failed to download requirements.txt"
     exit 1
   }
   
   # Install dependencies
   sudo pip3 install -r /home/hadoop/requirements.txt || {
     echo "Failed to install Python dependencies"
     exit 1
   }
   
   echo "Dependencies installed successfully"
   ```

This bootstrap script is crucial for making sure your EMR cluster has all the necessary dependencies to run your PySpark
code successfully.