# Configuration Errors

## Symptom

- Job fails immediately (within seconds)
- "Cannot find main class"
- "ClassNotFoundException"
- "Cannot access resource"

## Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| Cannot find main class | Wrong class path | Check spark-submit classpath |
| java.net.UnknownHostException | Host not found | Check SPARK_MASTER_IP |
| Cannot access s3:// | IAM permissions | Add EC2 IAM role |
| Caused by: org.apache.hadoop.fs.s3.S3Exception | S3 access denied | Check Glue permissions |

## Solutions

### Solution 1: Check JAR Files
```bash
# Make sure all JARs in --jars are correct paths
spark-submit \
    --jars /path/to/jar1.jar,/path/to/jar2.jar \
    my_job.py
```

### Solution 2: Fix SPARK_HOME
```bash
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
```

### Solution 3: Check EMR IAM Role
```bash
# On EMR, check EC2 instance profile
curl http://169.254.169.254/latest/meta-data/iam/security-credentials/
```

### Solution 4: Verify Environment
```bash
spark-submit --version
java -version
python --version
```

---

**See Also**: [Configuration Reference](../09-reference/configuration-reference.md)
