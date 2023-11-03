# NYC_LT

# Part 1 from NYC microsoft link provided
# Azure storage access info

blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "yellow"
blob_sas_token = "r"

# Allow SPARK to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set(
  'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
  blob_sas_token)
print('Remote blob path: ' + wasbs_path)

# SPARK read parquet, note that it won't load any data yet by now
df = spark.read.parquet(wasbs_path)
print('Register the DataFrame as a SQL temporary view: source')
df.createOrReplaceTempView('source')

# Display top 10 rows
print('Displaying top 10 rows: ')
display(spark.sql('SELECT * FROM source LIMIT 10'))


## Part 2
## dataframe and summary of required information

query = """

SELECT
    paymentType,
    puYear as year,
    puMonth as month,
    avg(fareAmount) as Mean_cost,
    percentile_cont(0.5) within group (order by fareAmount) Over
    (Partition By paymentType, puYear, puMonth) as median_cost,
    avg(totalAmount) as mean_prices,
    percentile_cont(0.5) within group (order by totalAmount) Over
    (Partition By paymentType, puYear, puMonth) as median_prices,
    avg(passengerCount) as mean_passengerCount,
    percentile_cont(0.5) within group (order by passengerCount) Over
    (Partition By paymentType, puYear, puMonth) as median_passengerCount
FROM source
GROUP BY paymentType, puYear, puMonth, fareAmount, totalAmount, passengerCount
"""

## Part 3
## saves data in csv format on user folder on Azure databricks
# note the dbfs location can be change as needed to run the file

dataview = spark.sql(query).write.csv("dbfs:/Users/oreoluwaa200@gmail.com")

