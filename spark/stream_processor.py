from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType

# --- Schéma JSON ---
schema = (StructType()
    .add("duration",        StringType())
    .add("protocol_type",   StringType())
    .add("service",         StringType())
    .add("flag",            StringType())
    .add("src_bytes",       StringType())
    .add("dst_bytes",       StringType())
    .add("land",            StringType())
    .add("wrong_fragment",  StringType())
    .add("urgent",          StringType())
    .add("hot",             StringType())
    .add("num_failed_logins", StringType())
    .add("logged_in",       StringType())
    .add("num_compromised", StringType())
    .add("root_shell",      StringType())
    .add("su_attempted",    StringType())
    .add("num_root",        StringType())
    .add("num_file_creations", StringType())
    .add("num_shells",      StringType())
    .add("num_access_files", StringType())
    .add("num_outbound_cmds", StringType())
    .add("is_host_login",   StringType())
    .add("is_guest_login",  StringType())
    .add("count",           StringType())
    .add("srv_count",       StringType())
    .add("serror_rate",     StringType())
    .add("srv_serror_rate", StringType())
    .add("rerror_rate",     StringType())
    .add("srv_rerror_rate", StringType())
    .add("same_srv_rate",   StringType())
    .add("diff_srv_rate",   StringType())
    .add("srv_diff_host_rate", StringType())
    .add("dst_host_count",  StringType())
    .add("dst_host_srv_count", StringType())
    .add("dst_host_same_srv_rate", StringType())
    .add("dst_host_diff_srv_rate", StringType())
    .add("dst_host_same_src_port_rate", StringType())
    .add("dst_host_srv_diff_host_rate", StringType())
    .add("dst_host_serror_rate", StringType())
    .add("dst_host_srv_serror_rate", StringType())
    .add("dst_host_rerror_rate", StringType())
    .add("dst_host_srv_rerror_rate", StringType())
    .add("label",           StringType())
    .add("difficulty",      StringType())
)

# --- Session Spark ---
spark = (SparkSession.builder
    .appName("SecuStream")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.3")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# --- Lecture Kafka ---
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "security-events")
    .option("startingOffsets", "latest")
    .load())

# --- Parse JSON ---
events = (raw
    .select(from_json(col("value").cast("string"), schema).alias("d"))
    .select("d.*"))

# --- 3 Règles de détection ---
alerts = events.filter(
    (col("label") != "normal") |
    (col("flag") == "S0")      |
    (col("serror_rate").cast("float") > 0.5)
).withColumn("rule",
    col("label")
)

# --- Sink PostgreSQL ---
PG_URL  = "jdbc:postgresql://localhost:5432/bigdata"
PG_PROPS = {
    "user":     "admin",
    "password": "secret",
    "driver":   "org.postgresql.Driver"
}

def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    batch_df.select(
        col("protocol_type").alias("protocol"),
        col("service"),
        col("flag"),
        col("src_bytes").cast("int").alias("src_bytes"),
        col("dst_bytes").cast("int").alias("dst_bytes"),
        col("label"),
        col("serror_rate").cast("float").alias("serror_rate"),
        col("rerror_rate").cast("float").alias("rerror_rate"),
        col("rule")
    ).write.jdbc(PG_URL, "alerts", mode="append", properties=PG_PROPS)
    print(f"Batch {batch_id} — {batch_df.count()} alertes écrites dans PostgreSQL")

# --- Sink Parquet ---
parquet_query = (events.writeStream
    .format("parquet")
    .option("path", "/tmp/bigdata/parquet")
    .option("checkpointLocation", "/tmp/bigdata/checkpoint_parquet")
    .outputMode("append")
    .start())

# --- Sink PostgreSQL via foreachBatch ---
pg_query = (alerts.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/bigdata/checkpoint_pg")
    .outputMode("update")
    .start())

print("Spark Streaming démarré — en attente d'événements Kafka...")
pg_query.awaitTermination()
