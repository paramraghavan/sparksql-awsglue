When using `aws emr add-step` with a Spark job, the supporting Python files that `myjob.py` imports need to be
accessible to all nodes in the EMR cluster. Here are the main approaches:

## 1. S3 Storage (Recommended)

Store supporting files in S3 and reference them in your step configuration:

```bash
aws emr add-steps --cluster-id j-XXXXXXXXX --steps '[{
  "Name": "My Spark Job",
  "Type": "Spark",
  "ActionOnFailure": "CONTINUE",
  "Args": [
    "--py-files", "s3://your-bucket/supporting-files.zip,s3://your-bucket/utils.py",
    "s3://your-bucket/myjob.py"
  ]
}]'
```

## 2. Zip Archive Method

Package all supporting files into a zip archive:

```bash
# Create zip with supporting modules
zip -r supporting-files.zip mymodule/ utils.py config.py

# Upload to S3
aws s3 cp supporting-files.zip s3://your-bucket/

# Reference in add-step
--py-files s3://your-bucket/supporting-files.zip
```

## 3. Bootstrap Actions

Install supporting files during cluster launch:

```bash
aws emr create-cluster \
  --bootstrap-actions Path=s3://your-bucket/install-deps.sh \
  # ... other cluster config
```

## 4. File Structure Example

If your local structure is:

```
project/
├── myjob.py
├── utils/
│   ├── __init__.py
│   └── helpers.py
└── config.py
```

Upload everything to S3:

```bash
aws s3 cp myjob.py s3://your-bucket/
aws s3 sync utils/ s3://your-bucket/utils/
aws s3 cp config.py s3://your-bucket/
```

Then use:

```bash
--py-files s3://your-bucket/utils/,s3://your-bucket/config.py
```

## Key Points:

- **`--py-files`** distributes Python files to all worker nodes
- Files are automatically added to the Python path
- S3 paths are downloaded to each node at job start
- Zip files are automatically extracted
- Use comma-separated list for multiple files/directories

The most common pattern is packaging everything into a zip file and using the `--py-files` argument to distribute it
across the cluster.