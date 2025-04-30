# Amazon EMR Deployment Guide for PySpark Customer Segmentation

This guide walks through the step-by-step process of deploying your customer segmentation PySpark script to Amazon EMR.

## 1. Prepare Your Code Structure

Your current structure seems appropriate, with a main script and supporting modules in the `src` directory. Ensure your directory structure is as follows:

```
project_root/
├── main_script.py (your main script)
├── src/
│   ├── utils/
│   │   ├── spark_utils.py
│   │   └── model_io.py
│   ├── data/
│   │   └── preprocessing.py
│   ├── features/
│   │   └── build_features.py
│   └── models/
│       └── clustering.py
└── requirements.txt (create this file)
```

## 2. Create a requirements.txt File

Create a `requirements.txt` file listing all required dependencies:

```
pyspark==3.3.0
numpy==1.22.4
pandas==1.5.0
scikit-learn==1.1.3
# Add any other dependencies your code uses
```

## 3. Package Your Code

Bundle your code as a zip file for easy deployment:

```bash
zip -r customer_segmentation.zip main_script.py src/ requirements.txt
```

## 4. Upload Code to S3

Upload your code package to an S3 bucket:

```bash
aws s3 cp customer_segmentation.zip s3://my-bucket/code/customer_segmentation.zip
```

## 5. Create an EMR Cluster

Use the AWS CLI or console to create an EMR cluster:

### Using AWS CLI:

```bash
aws emr create-cluster \
    --name "Customer Segmentation Cluster" \
    --release-label emr-6.10.0 \
    --applications Name=Spark \
    --ec2-attributes KeyName=myKey \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --bootstrap-actions Path="s3://my-bucket/bootstrap/install_dependencies.sh" \
    --log-uri "s3://my-bucket/logs/" \
    --use-default-roles
```

### Bootstrap Script (install_dependencies.sh)

Create a bootstrap script to install your dependencies:

```bash
#!/bin/bash
sudo pip3 install -r /home/hadoop/requirements.txt
```

Upload this script to S3:

```bash
aws s3 cp install_dependencies.sh s3://my-bucket/bootstrap/install_dependencies.sh
```

## 6. Submit Your Spark Job

Submit your Spark job to the cluster:

### Using AWS CLI:

```bash
aws emr add-steps \
    --cluster-id j-XXXXXXXXXXXX \
    --steps Type=Spark,Name="Customer Segmentation",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,s3://my-bucket/code/main_script.py]
```

### Using spark-submit on EMR (after SSH into master node):

```bash
spark-submit \
    --deploy-mode cluster \
    --master yarn \
    --py-files s3://my-bucket/code/customer_segmentation.zip \
    s3://my-bucket/code/main_script.py
```

## 7. Set Up EMR Steps for Production

For a production pipeline, it's recommended to set up automated EMR steps:

1. Create a JSON configuration file `job_config.json`:

```json
[
  {
    "Name": "Customer Segmentation",
    "Type": "Spark",
    "ActionOnFailure": "CONTINUE",
    "Args": [
      "spark-submit",
      "--deploy-mode", "cluster",
      "--master", "yarn",
      "--py-files", "s3://my-bucket/code/customer_segmentation.zip",
      "s3://my-bucket/code/main_script.py"
    ]
  }
]
```

2. Use this configuration to launch your job:

```bash
aws emr add-steps \
    --cluster-id j-XXXXXXXXXXXX \
    --steps file://job_config.json
```

## 8. Automate with AWS Step Functions or Airflow

For full production pipelines, consider:

- AWS Step Functions to orchestrate the entire workflow
- Apache Airflow for complex scheduling and dependencies
- AWS EventBridge for event-driven execution

## 9. Monitoring and Logging

- Configure CloudWatch for cluster and job monitoring
- Ensure your application's logs are captured in S3
- Add appropriate error handling in your code

## 10. Cost Optimization

- Use Spot Instances for non-critical workloads
- Set up auto-termination for the cluster after job completion
- Consider using EMR Serverless for serverless Spark execution

## Notes for Production Use

- Consider using AWS Glue Data Catalog with your EMR clusters
- Implement appropriate IAM roles and security measures
- Use EMR Managed Scaling for dynamic resource allocation
- Consider EMR Studio for collaborative development
