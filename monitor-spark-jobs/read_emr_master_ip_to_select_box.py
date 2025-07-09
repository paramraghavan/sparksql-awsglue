import streamlit as st
import boto3
import pandas as pd


@st.cache_data
def get_emr_master_ips():
    """Get EMR master node IPs with caching for better performance"""
    # Create EMR client
    emr_client = boto3.client('emr')
    ec2_client = boto3.client('ec2')

    master_ips = []

    try:
        # Get all EMR clusters
        response = emr_client.list_clusters(
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        )

        for cluster in response['Clusters']:
            cluster_id = cluster['Id']
            cluster_name = cluster['Name']

            # Get cluster details
            cluster_detail = emr_client.describe_cluster(ClusterId=cluster_id)

            # Get master instance ID
            master_instance_id = cluster_detail['Cluster']['Ec2InstanceAttributes']['Ec2InstanceId']

            if master_instance_id:
                # Get instance details from EC2
                ec2_response = ec2_client.describe_instances(InstanceIds=[master_instance_id])

                for reservation in ec2_response['Reservations']:
                    for instance in reservation['Instances']:
                        private_ip = instance.get('PrivateIpAddress')
                        public_ip = instance.get('PublicIpAddress')

                        master_ips.append({
                            'cluster_id': cluster_id,
                            'cluster_name': cluster_name,
                            'master_instance_id': master_instance_id,
                            'private_ip': private_ip,
                            'public_ip': public_ip
                        })

    except Exception as e:
        st.error(f"Error fetching EMR clusters: {e}")
        return []

    return master_ips


def main():
    st.title("EMR Master Node IP Selector")
    st.markdown("Select an EMR cluster to view its master node IP address")

    # Add a refresh button
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("🔄 Refresh"):
            st.cache_data.clear()
            st.rerun()

    # Get EMR master IPs
    with st.spinner("Fetching EMR clusters..."):
        master_nodes = get_emr_master_ips()

    if not master_nodes:
        st.warning("No active EMR clusters found or unable to fetch cluster information.")
        st.info("Make sure you have:")
        st.markdown("""
        - AWS credentials configured
        - Proper IAM permissions (emr:ListClusters, emr:DescribeCluster, ec2:DescribeInstances)
        - Active EMR clusters in your AWS account
        """)
        return

    # Create options for selectbox
    cluster_options = []
    cluster_lookup = {}

    for node in master_nodes:
        display_name = f"{node['cluster_name']} ({node['cluster_id']})"
        cluster_options.append(display_name)
        cluster_lookup[display_name] = node

    # Streamlit selectbox
    st.subheader("Select EMR Cluster:")
    selected_cluster = st.selectbox(
        "Choose a cluster:",
        options=cluster_options,
        index=0 if cluster_options else None,
        help="Select an EMR cluster to view its master node details"
    )

    # Display selected cluster information
    if selected_cluster and selected_cluster in cluster_lookup:
        node = cluster_lookup[selected_cluster]

        st.subheader("Master Node Details:")

        # Create columns for better layout
        col1, col2 = st.columns(2)

        with col1:
            st.info(f"**Cluster Name:** {node['cluster_name']}")
            st.info(f"**Cluster ID:** {node['cluster_id']}")
            st.info(f"**Master Instance ID:** {node['master_instance_id']}")

        with col2:
            st.success(f"**Private IP:** {node['private_ip'] or 'Not available'}")
            if node['public_ip']:
                st.success(f"**Public IP:** {node['public_ip']}")
            else:
                st.warning("**Public IP:** Not available")

        # Additional actions
        st.subheader("Quick Actions:")
        col1, col2, col3 = st.columns(3)

        with col1:
            if node['private_ip']:
                if st.button("📋 Copy Private IP"):
                    st.write(f"Private IP: `{node['private_ip']}`")
                    st.success("Private IP ready to copy!")

        with col2:
            if node['public_ip']:
                if st.button("📋 Copy Public IP"):
                    st.write(f"Public IP: `{node['public_ip']}`")
                    st.success("Public IP ready to copy!")

        with col3:
            if st.button("🔗 SSH Command"):
                ip_to_use = node['public_ip'] if node['public_ip'] else node['private_ip']
                if ip_to_use:
                    ssh_command = f"ssh -i your-key.pem hadoop@{ip_to_use}"
                    st.code(ssh_command, language='bash')
                    st.info("Replace 'your-key.pem' with your actual key file")

    # Display all clusters in a table
    st.subheader("All EMR Clusters:")
    df = pd.DataFrame(master_nodes)

    # Reorder columns for better display
    if not df.empty:
        df_display = df[['cluster_name', 'cluster_id', 'private_ip', 'public_ip', 'master_instance_id']]
        df_display.columns = ['Cluster Name', 'Cluster ID', 'Private IP', 'Public IP', 'Master Instance ID']
        st.dataframe(df_display, use_container_width=True)

    # Add export functionality
    if master_nodes:
        st.subheader("Export Data:")
        col1, col2 = st.columns(2)

        with col1:
            # Export as CSV
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name="emr_master_ips.csv",
                mime="text/csv"
            )

        with col2:
            # Export as JSON
            json_data = df.to_json(orient='records', indent=2)
            st.download_button(
                label="📥 Download as JSON",
                data=json_data,
                file_name="emr_master_ips.json",
                mime="application/json"
            )


if __name__ == "__main__":
    main()
