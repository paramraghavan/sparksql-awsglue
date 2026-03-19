# AWS-Specific Errors

## Common EMR/Glue/S3 Errors

### S3 Access Denied
```
com.amazonaws.AmazonServiceException: User is not authorized
```

**Fix**:
1. Check EC2 instance IAM role has S3 permissions
2. Verify bucket name is correct
3. Check path format: `s3://bucket/path/` (not `s3a://`)

### Glue Catalog Table Not Found
```
Table not found: my_table
```

**Fix**:
1. Create table in Glue Catalog: `CREATE TABLE my_table ...`
2. Run Glue Crawler
3. Check database name: `spark.catalog.setCurrentDatabase("my_db")`

### Athena Query Failed
```
Query execution error
```

**Check**:
1. Table exists in Glue Catalog
2. Partitions are registered
3. Data format is readable (Parquet, CSV, JSON)

### Redshift Connection Failed
```
Connection refused
```

**Check**:
1. Cluster endpoint is correct
2. Security group allows access
3. Glue credentials are valid

## EMR-Specific Issues

### Node Bootstrap Failed
Check bootstrap script output in EMR logs

### YARN Resource Limits
Job keeps getting killed - increase YARN limits or split into smaller jobs

---

**See Also**: [AWS Integration](../05-aws-integration/)
