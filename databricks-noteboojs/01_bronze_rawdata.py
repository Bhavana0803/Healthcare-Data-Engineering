from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "<<Namespace_hostname>>"
event_hub_name = "Eventhub_name"
event_hub_connection_str = "<<Connection_string>>"
kafka_options = {
    'kafka.bootstrap.servers': f'{event_hub_namespace}:9093',
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_connection_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

# Read from Event Hub
raw_df = spark.readStream.format("kafka").options(**kafka_options).load()

# Cast to JSON string
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

# ADLS Configuration
spark.conf.set(
    "fs.azure.account.key.<<storageaccountname>>.dfs.core.windows.net",
   <<storage_account_access_key>>
)

bronze_path     = "abfss://<<container>>@<<Storage_account_name>>.dfs.core.windows.net/<<path>>"
checkpoint_path = "abfss://<<container>>@<<Storage_account_name>>.dfs.core.windows.net/checkpoints/<<path>>"

# ✅ FIX: Assign .start() to query variable
query = (
    json_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="30 seconds")
    .start(bronze_path)  # ✅ .start() returns the query object
)

query.awaitTermination()  # ✅ Now this works