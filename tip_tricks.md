# Tips and Tricks

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
- By default, the Fair Scheduler bases scheduling fairness decisions only on memory. It can be configured to schedule
  with both memory and CPU. When there is a single app running, that app uses the entire cluster. When other apps are
  submitted, resources that free up are assigned to the new apps, so that each app eventually on gets roughly the same
  amount of resources. The Fair Scheduler lets all apps run by default, but it is also possible to limit the number of
  running apps per user and per queue through the config file. This can be useful when a user must submit hundreds of
  apps at once, or in general to improve performance if running too many apps at once would cause too much intermediate
  data to be created or too much context-switching. Limiting the apps does not cause any subsequently submitted apps to
  fail, only to wait in the scheduler’s queue until some of the user’s earlier apps finish.
- let's say we use the default queue, it is best to limit the maxRunningApps. 
- And at the job/spark-submit level limit the executor-memory to 32gb(try with 16 or 32), available values 8,16,32,64 gb
  and for num-executors try with 4 or 5 and see how it goes. Num of executors could be upto 20. **Todo** How to use
  executor-cores wrt to num-executors?
- [More of fair scheduler](https://hadoop.apache.org/docs/r2.7.1/hadoop-yarn/hadoop-yarn-site/FairScheduler.html)
- All the above are the yarn scheduler properties.