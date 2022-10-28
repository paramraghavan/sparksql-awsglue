Apache DistCp is an open-source tool you can use to copy large amounts of data. S3DistCp is similar to DistCp, but optimized to work with AWS, 
particularly Amazon S3. The command for S3DistCp in Amazon EMR version 4.0 and later is s3-dist-cp, which you add as a step in a cluster or at the 
command line. Using S3DistCp, you can efficiently copy large amounts of data from Amazon S3 into HDFS where it can be processed by subsequent steps in
your Amazon EMR cluster. You can also use S3DistCp to copy data between Amazon S3 buckets or from HDFS to Amazon S3. S3DistCp is more scalable and 
efficient for parallel copying large numbers of objects across buckets and across AWS accounts.

For specific commands that demonstrate the flexibility of S3DistCP in real-world scenarios, see Seven tips for using S3DistCp on the AWS Big Data blog.

Like DistCp, S3DistCp uses MapReduce to copy in a distributed manner. It shares the copy, error handling, recovery, and reporting tasks across
several servers. For more information about the Apache DistCp open source project, see the DistCp guide in the Apache Hadoop documentation.

(more details)[https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html]
