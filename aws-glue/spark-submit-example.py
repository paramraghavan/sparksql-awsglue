import awswrangler as wr


print(f'Test Run')
# now reading back data after the columns - departure and arrival have been
# deleted from  table/glue catalog, note these columns still exist in the s3 bucket
df = wr.athena.read_sql_query("SELECT * FROM flights WHERE CAST(distance as decimal) > 2500.00 ",
                              database="sample_db",
                              ctas_approach=True)
print(df.to_string())
