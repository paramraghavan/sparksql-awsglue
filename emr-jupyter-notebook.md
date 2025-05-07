# have emr with jupyterenterprisegateway 2.6.0 installed, how do i connect this emr with jupyter notebook

## JupyterHub Architecture on Amazon EMR

When you create an EMR cluster with JupyterHub, Amazon EMR deploys a Docker container on the master node of your cluster
that contains JupyterHub, all the components required for Jupyter, and Sparkmagic. This containerized approach provides
several benefits:

1. **Isolation**: The JupyterHub environment is isolated from the rest of the system, ensuring dependencies don't
   conflict with other software on the master node.

2. **Portability**: The Docker container provides a consistent environment regardless of the underlying EMR version or
   configuration.

3. **Security**: The container helps enforce security boundaries between JupyterHub users and the underlying system.

The Docker container is named `jupyterhub` and runs the Ubuntu operating system inside it, separate from the Amazon
Linux that powers the EMR nodes. This is why we need to use Docker commands to manage JupyterHub services:

```bash
sudo docker restart jupyterhub   # Restart the container
sudo docker exec jupyterhub command   # Run a command inside the container
```

## How Components Connect

Inside this Docker container:

1. JupyterHub manages multiple user instances
2. Each user gets their own Jupyter notebook server
3. Sparkmagic connects to Apache Livy (which runs outside the container on the EMR cluster)
4. Apache Livy acts as a REST server for Spark, allowing notebook code to execute on the cluster

## Configuration Files in the Container

When you make changes to configuration files like `/etc/jupyter/jupyter_notebook_config.py`, these changes affect the
configuration inside the Docker container. This is why you need to restart the container after making changes - to
ensure the JupyterHub service inside the container picks up the new configuration.

## Customization Considerations

One important thing to note is that customizations you perform within the container may not persist if the container
restarts. For permanent changes, it's recommended to create scripts that automate your container configuration, which
can be applied whenever the container needs to be restarted.

This containerized approach simplifies the deployment and management of JupyterHub on EMR while providing a consistent
and isolated environment for data science workloads.

To connect your EMR (Elastic MapReduce) cluster with Jupyter Notebook using JupyterEnterpriseGateway 2.6.0, you'll need
to follow these steps:

1. First, ensure your EMR cluster is properly configured with JupyterEnterpriseGateway 2.6.0 installed.

2. You have two main options to connect:

## Option 1: Using the Kernel Gateway Client

You can connect to your EMR cluster from your local Jupyter Notebook using the `jupyter_kernel_gateway` client:

```python
# Install the client if you haven't already
# pip install jupyter_kernel_gateway

# In your notebook or jupyter_notebook_config.py
c.KernelGatewayClient.url = "http://emr-master-node-dns:8888"  # Replace with your EMR master node DNS and port
```

```markdown
In AWS EMR, the jupyter_notebook_config.py file is typically stored in the /etc/jupyter/ directory on the master node by
default. This file is then copied to the jupyterhub container as the default configuration Amazon.
When working with JupyterHub on EMR, you might also need to be aware of a few related configuration files:

/etc/jupyter/conf/ directory - This contains other configuration files like SSL certificates that you might want to
replace with your own trusted certificates instead of the default self-signed ones Amazon
~/.jupyter/jupyter_notebook_config.py - Some users create a configuration file in this location under a specific user's
home directory to customize settings for that user GitHub

If you're looking to modify the configuration, you can edit this file to change various settings like:

SSL certificates and keys
Port settings (default is 9443 for JupyterHub)
Content manager configurations

For any major configuration changes, make sure to restart the Jupyter/JupyterHub services after modifying the config
file for the changes to take effect

# Connect to the master node via SSH first, then to restart run the following:

sudo docker restart jupyterhub

```

## Option 2: Setting up a direct connection

1. SSH into your EMR master node or create an SSH tunnel:
   ```bash
   ssh -i your-key.pem -L 8888:localhost:8888 hadoop@emr-master-node-dns
   ```

2. Configure your Jupyter Notebook to use the Enterprise Gateway as its kernel provider:
   ```bash
   jupyter notebook --gateway-url=http://localhost:8888
   ```

## Option 3: Using AWS Console integration

If you're using AWS EMR's Jupyter integration:

1. Go to your EMR cluster in the AWS console
2. Click on "Applications" tab
3. Find JupyterEnterpriseGateway in the list and click on "Connect"
4. Follow the prompts to create an SSH tunnel and connect

For more specific configuration, you might need to modify the JupyterEnterpriseGateway configuration files on your EMR
cluster located in `/etc/jupyter/jupyter_enterprise_gateway_config.py`.

## how do i connect from my pcs chrome browser

To connect to your EMR cluster's JupyterEnterpriseGateway 2.6.0 from your PC's Chrome browser, you'll need to set up an
SSH tunnel and then access Jupyter through your browser. Here's how to do it:

## Setup SSH Tunnel

1. **Set up an SSH tunnel from your PC to the EMR master node**:

   **For Windows users**:
    - Install PuTTY or use Windows Subsystem for Linux (WSL)
    - In PuTTY, set:
        - Host Name: `hadoop@emr-master-node-dns` (replace with your actual EMR master node address)
        - Under Connection > SSH > Tunnels:
            - Source port: `8888`
            - Destination: `localhost:8888`
            - Click "Add" then "Open"
        - Enter your key when prompted

   **For Mac/Linux users**:
   ```bash
   ssh -i your-key.pem -L 8888:localhost:8888 hadoop@emr-master-node-dns
   ```
   (Replace `your-key.pem` with your actual key file and `emr-master-node-dns` with your EMR master node address)

2. **Keep this SSH session open** while you're using Jupyter

## Access Jupyter in Chrome

1. Open Chrome and navigate to:
   ```
   http://localhost:8888
   ```

2. You might be prompted for a token or password. If you don't know it:
    - In your SSH session, run:
      ```bash
      jupyter notebook list
      ```
    - This will show running notebooks with their tokens
    - Copy the token from the output (it looks like `?token=abc123xyz...`)

3. If you're using JupyterHub with the Enterprise Gateway, you may need to:
   ```
   http://localhost:8888/hub/login
   ```

## Alternative: Using AWS EMR Studio

If your organization has set up EMR Studio (which provides a managed Jupyter environment):

1. Go to AWS Console > EMR > EMR Studio
2. Open your Studio
3. Create a new workspace linked to your EMR cluster
4. Access directly via browser without SSH tunneling

