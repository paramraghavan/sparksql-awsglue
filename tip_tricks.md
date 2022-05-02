# Tips and Tricks
1.**Cache/perist dataframe**. df.cache() or df.persist() 
- If you are using a dataframe result over and over again, use cache. Let's say for example 
you create an aggregation based on a bucnh of tables or data frames and you use this result 
  over and over again in you next 4 steps. It better cache the resultant dataframe instead of
  calculating it over and over and over again for each of the following 4 steps.

- Another example, let's say we read a really large dataset with let's say 600gb data. 
On this dataset we apply about 10 different transforms, and then perform a count
   on the resulting dataframe and in the very next step we write this computed dataframe
   to the disk, so here is what is happening
  - 1st step read the 600 GB data into dataframe df
  - 2nd apply transform to this data frame, like adding extra columns 
  - 3rd step join this dataframe with another dataframe
  - ..  
  - 10th, do some transform to dataframe, **df**
  - 11th, next **apply count on dataframe df**, note this is an action step.
  - 12th, next you write this dataframe to the disk, this is also considered aan actions step   
- Note that when we do the write in 12th step, the spark engine has to apply all the transformations from step 1 thru step 10
  all over again, which is compute expensive, especially for large volume of data
- So between step 10 and 11 we can add step 10.5 wherein we cache the dataframe, df.persist(). So at step 12, the write
  operation uses the cached data to write to disk.   
