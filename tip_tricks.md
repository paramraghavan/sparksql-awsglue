# Tips and Tricks

## What is EMR

The central component of Amazon EMR is the cluster. A cluster is a collection of Amazon Elastic Compute Cloud (Amazon
EC2) instances. Each instance in the cluster is called a node. Each node has a role within the cluster, referred to as
the node type. Amazon EMR also installs different software components on each node type, giving each node a role in a
distributed application like Apache Hadoop.

The node types in Amazon EMR are as follows:
- Master node: A node that manages the cluster by running software components to coordinate the distribution of data and
tasks among other nodes for processing. The master node tracks the status of tasks and monitors the health of the
cluster. Every cluster has a master node, and it's possible to create a single-node cluster with only the master node.

- Core node: A node with software components that run tasks and store data in the Hadoop Distributed File System (HDFS) on
your cluster. Multi-node clusters have at least one core node.

- Task node: A node with software components that only runs tasks and does not store data in HDFS. Task nodes are
optional.

![img_5.png](img_5.png)

[ref](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-overview.html)

- EMR  can run [various jobs](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-ha-applications.html) - spark, tensorflow, hadoop, hive etc. In out case we are going to focus on Spark jobs. We
submit spark jobs and we use the yarn scheduler to submit our jobs.

## Yarn Scheduler
Apache YARN (Yet Another Resource Negotiator) is a cluster resource management platform for distributed computing
paradigms which is most often used with Hadoop but it is general enough to be used with other platforms like spark
in our case here.

A scheduler typically handles the resource allocation of the jobs submitted to YARN. In simple words — for example — if
a computer app/service wants to run and needs 1GB of RAM and 2 processors for normal operation — it is the job of YARN
scheduler to allocate resources to this application in accordance to a defined policy.

**There are three types of schedulers available in YARN**: FIFO, Capacity and Fair. FIFO (first in, first out) is the
simplest to understand and does not need any configuration. It runs the applications in submission order by placing them
in a queue. Application submitted first, gets resources first and upon completion, the scheduler serves next application
in the queue. However, _FIFO is not suited for shared clusters as large applications will occupy all resources and queues
will get longer due to lower serving rate._

![img_6.png](img_6.png)

Above diagram represents the difference among three schedulers. It is now evident that in case of FIFO, a small job blocks
until the large job complete. Capacity scheduler maintains a separate queue for small jobs in order to start them as
soon a request initiates. However, this comes at a cost as we are dividing cluster capacity hence large jobs will take
more time to complete.

**Fair scheduler** does not have any requirement to reserve capacity. It dynamically balances the resources into all
accepted jobs. It can be configured to schedule
with both memory and CPU. When there is a single app running, that app uses the entire cluster. When other apps are
submitted, resources that free up are assigned to the new apps, so that each app eventually on gets roughly the same
amount of resources. This eliminates both drawbacks as seen in FIFO and capacity scheduler i.e. overall effect is
timely completion of small jobs with high cluster utilization. the Fair Scheduler bases scheduling fairness decisions
only on memory.The Fair Scheduler lets all apps run by default,
but it is also possible to limit the number of running apps per user and per queue through the config file. This can
be useful when a user must submit hundreds of apps at once, or in general to improve performance if running too many
apps at once would cause too much intermediate data to be created or too much context-switching. Limiting the apps does
not cause any subsequently submitted apps to fail, only to wait in the scheduler’s queue until some of the user’s
earlier apps finish.

[ref](https://towardsdatascience.com/schedulers-in-yarn-concepts-to-configurations-5dd7ced6c214)
[configuring scheduler](https://medium.com/@sohamghosh/schedulers-in-emr-6445180b44f6)


## Scenarios

1.**Cache/perist dataframe**. df.cache() or df.persist()

- If you are using a dataframe result over and over again, use cache. Let's say for example you create an aggregation
  based on a bucnh of tables or data frames and you use this result over and over again in you next 4 steps. It better
  cache the resultant dataframe instead of calculating it over and over and over again for each of the following 4
  steps.

- Another example, let's say we read a really large dataset with let's say 600gb data. On this dataset we apply about 10
  different transforms, and then perform a count on the resulting dataframe and in the very next step we write this
  computed dataframe to the disk, so here is what is happening
    - 1st step read the 600 GB data into dataframe df
    - 2nd apply transform to this data frame, like adding extra columns
    - 3rd step join this dataframe with another dataframe
    - ..
    - 10th, do some transform to dataframe, **df**
    - 11th, next **apply count on dataframe df**, note this is an action step.
    - 12th, next you write this dataframe to the disk, this is also considered aan actions step
- Note that when we do the write in 12th step, the spark engine has to apply all the transformations from step 1 thru
  step 10 all over again, which is compute expensive, especially for large volume of data
- So between step 10 and 11 we can add step 10.5 wherein we cache the dataframe, df.persist(). So at step 12, the write
  operation uses the cached data to write to disk.

2. Optimize spark submit
- Use fair scheduler for you cluster, this way all the jobs submitted are getting equal resources
- Configure the scheduler at the cluster level, let's say we use the default queue, it is best to limit the maxRunningApps.
  You can either update the fair-scheduler.xml under /etc/hadoop/conf or add fair-scheduler classification to the Configurations
- <pre>
          {
            "Classification": "fair-scheduler",
            "allocations":  {
               "queue": [
                  {
                     "@name": "primary_queue",
                     "maxRunningApps": "50",
                     "schedulingPolicy": "fair",
                  }
               ],
               "queuePlacementPolicy": [
                  {
                     "@name": "default",
                     "@queue": "primary_queue"
                  }
               ]
            }
          }
</pre>
- Or you have the file fair-scheduler.xmlin /etc/hadoop/conf, this is an example of another configuration using the root hadoop queue
<pre>
<allocations>
    <!-- sets the default running app limit for queues; overridden by maxRunningApps element in each queue.-->
    <queueMaxAppsDefault>50</queueMaxAppsDefault>
    <!-- sets the default AM resource limit for queue; overridden by maxAMShare element in each queue. -->
    <queueMaxAMShareDefault>0.9</queueMaxAMShareDefault>
</allocations>
</pre>
  
- Following are the options available:
<pre>
          {
            "Classification": "fair-scheduler",
            "allocations":  {
               "queue": [
                  {
                     "@name": "primary_queue",
                     "minResources": "10000 mb,0vcores",
                     "maxResources": "90000 mb,0vcores",
                     "maxRunningApps": "50",
                     "maxAMShare": "0.1",
                     "weight": "2.0",
                     "schedulingPolicy": "fair",
                     "queue": {
                        "@name": "priimary_sub_queue",
                        "aclSubmitApps": "charlie",
                        "minResources": "5000 mb,0vcores"
                     }
                  },
                  {
                     "@name": "secondary_queue",
                     "@type": "parent",
                     "weight": "3.0"
                  }
               ],
               "queueMaxAMShareDefault": "0.5",
               "queueMaxResourcesDefault": "40000 mb,0vcores",
               "user": {
                  "@name": "sample_user",
                  "maxRunningApps": "30"
               },
               "userMaxAppsDefault": "5",
               "queuePlacementPolicy": [
                  {
                     "@name": "specified"
                  },
                  {
                     "@name": "primaryGroup",
                     "@create": "false"
                  },
                  {
                     "@name": "nestedUserQueue",
                     "rule": {
                        "@name": "secondaryGroupExistingQueue",
                        "@create": "false"
                     }
                  },
                  {
                     "@name": "default",
                     "@queue": "primary_queue"
                  }
               ]
            }
          }
</pre>
- And at the job/spark-submit level limit the executor-memory to 32gb(try with 16 or 32), available values 8,16,32,64 gb
  and for num-executors try with 4 or 5 and see how it goes. Num of executors could be upto 20. **Todo** How to use
  executor-cores wrt to num-executors?
- [More of fair scheduler](https://hadoop.apache.org/docs/r2.7.1/hadoop-yarn/hadoop-yarn-site/FairScheduler.html)
- https://amalgjose.com/2015/07/24/configuring-fair-scheduler-in-hadoop-cluster/
- All the above are the yarn scheduler properties.
- [performance][https://towardsdatascience.com/apache-spark-performance-boosting-e072a3ec1179]

3. example:
<pre>
val df = spark.read().format("csv").csv("/path/file.csv")

df = df.filter(....)  // you are over writing the df reference

// df.cache() // use this if filter and read is to be performed only once.

val cnt = df.count()  // action - triggers the filter and read operations 

df.saveAsTable()   //  action - triggers the filter and read operations 
</pre>
 Whenever a "action" (count() and saveAsTable() in this case) is performed it triggers the re-computation of data till the previous DF. 
That is, unless the previous DF is cached. If we have to avoid this reconstruction process everytime and if we start persisting the DF's, 
will we not end up occupying all the memory and spilling some data to disk and slow down the spark application?. _Yes its wasteful and thats 
why its advisable to cache the DF if its used more than once._ That's the reason all DFs are not persisted by default. However, when you 
persist there are multiple options like MEMORY_ONLY, MEMORY_AND_DISK besides others. Check details [here](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)
ref: https://stackoverflow.com/questions/63086480/are-dataframes-created-every-time-using-dag-when-the-df-is-referenced-multiple.

**Note:** after you use df.cache(), unpersist the cached df as soona s you do not need it.
<pre>
df.unpersist(false) // unpersists the Dataframe without blocking
</pre>

4. Yarn commands
<pre>
yarn application -help
yarn application appId <Applciaiton ID>
yarn application -appStates <States>
yarn application -appTags <Tags>
yarn application -appTypes <Types>
yarn application components <Components Name>
yarn application -decommission < Application Name>
yarn application - instances<Component Instances>
yarn application -list
yarn application -kill <application id>

sudo stop hadoop-yarn-resourcemanager
sudo start hadoop-yarn-resourcemanager
</pre>

5. Stop all jobs
for x in $(yarn application -list -appStates RUNNING | awk 'NR > 2 { print $1 }'); do yarn application -kill $x; done
