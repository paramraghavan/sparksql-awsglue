# emr spark-submit vs aws emr add-step

## emr spark-submit

- **Direct submission**: Runs Spark jobs directly on an active EMR cluster
- **Interactive**: Provides immediate feedback and logs in your terminal
- **SSH required**: You must SSH into the EMR master node to use this command
- **Real-time monitoring**: You can see job progress and logs as they execute
- **Best for**: Development, debugging, and interactive analysis

```bash
# Example (run from EMR master node)
spark-submit --class MyApp --master yarn myapp.jar
```

## aws emr add-step

- **Remote submission**: Submits jobs to EMR clusters from anywhere using the AWS CLI
- **Asynchronous**: Jobs are queued as "steps" and executed by the cluster
- **No SSH needed**: Can be run from your local machine or CI/CD pipelines
- **Step management**: Jobs become trackable steps in the EMR console
- **Best for**: Production workflows, automation, and remote job submission

```bash
# Example (run from anywhere with AWS CLI)
aws emr add-steps --cluster-id j-XXXXXXXXX --steps Type=Spark,Name="My Spark Job",ActionOnFailure=CONTINUE,Args=[--class,MyApp,s3://mybucket/myapp.jar]
```

## When to use each:

**Use `emr spark-submit`** when you're developing interactively on the cluster and need immediate feedback.

**Use `aws emr add-step`** when you're running production jobs, automating workflows, or submitting jobs remotely
without SSH access.

Both ultimately run the same Spark applications, but `add-step` provides better integration with AWS services and
workflows.