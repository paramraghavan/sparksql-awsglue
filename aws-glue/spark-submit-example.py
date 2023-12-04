'''
set PYSPARK_PYTHON=python

To run a standalone Python script, run the spark-submit utility and specify the path of your Python script as well 
as any arguments your Python script needs in the Command Prompt.
For example, to run the sample.py script with aruguments arg1, arg2, you can run the following command:

bin\spark-submit --verbose sample.py arg1 arg2

spark-submit --verbose aws-glue/spark-submit-example.py

make sure you have started jupyter notebook, open aws-glue.ipynb, enable pyspark and intiailize SparkContext

'''

import awswrangler as wr


print('tying to get handle to yarn Application id')
from pyspark.sql import SparkSession

'''
Gets an existing SparkSession or, if there is no existing one, creates a new one based 
on the options set in this builder.
'''
spark = SparkSession \
    .builder \
    .getOrCreate()
print(f'yarn application id : {spark.sparkContext.applicationId} ')

print(f'Test Run')

# now reading back data after the columns - departure and arrival have been
# deleted from  table/glue catalog, note these columns still exist in the s3 bucket
df = wr.athena.read_sql_query("SELECT * FROM flights WHERE CAST(distance as decimal) > 2500.00 ",
                              database="sample_db",
                              ctas_approach=True)
print(df.to_string())

