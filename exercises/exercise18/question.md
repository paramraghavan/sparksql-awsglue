submitting spark job using spark-submit 
with options --deploy-mode client --master yarn ==driver-memeory 32g --executorc-cores 4 --num-executors 72 --conf.spark.dynamicAllocatiob.enabled=false ./myjob.py

I have 122 task  nodes on the cluster, bu the job is only using 3-4, why is it not using more modes. Ths job is running for a long time
Also i see some pandas operation, pyspark df converted to pandas
dfm = df_sample(['AssignedModel']).distinct().toPandas()
and a loop
for n in dfm.AssignedModel.values:
  its going in a loop until n is > 15
   doing lots of logic  about 70 lines of code

Is it possible to do the above without converting pyspark df to pandas df and will it help