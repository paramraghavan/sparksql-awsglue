Emr jobs taking very long time to complete

When I looked stage looking at task I see

lots of active task with as following:

no disk spills , 15 active input 670 mb shuffle read 295gb and shuffle write 4.1gb its running for 64 hours finally it
runs to completion

I know which line of code is causing this issue. This job used to run all good until they ingested new data. Now the
data set grown in size t o about 28 gb from about 1 or 2 gb, lets say this dataset at input_df

To line which is causing the problem is taking the input_df - lets call this function process, when the input_df is
small, it runs to completion very quick now with this 28gb input db - this is in s3 and is split into 1000 parquet file.

How can I go about fixing without making any changes to the function process

EMR is activy resising from 9 to 42 nodes, and then sizing back to 7 task nodes, this happens continusodsly