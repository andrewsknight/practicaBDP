package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

import scala.concurrent.duration.Duration

object AntennaStreamingJob extends StreamingJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Final Exercise SQL Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    /*val jsonSchema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("metric", StringType, nullable = false),
      StructField("value", IntegerType, nullable = false)
    )
    )*/
    val antennaMessageSchema: StructType = ScalaReflection.schemaFor[AntennaMessage].dataType.asInstanceOf[StructType]

    dataFrame
      .select(from_json(col("value").cast(StringType), antennaMessageSchema).as("json"))
      .select("json.*")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("antenna")
      .join(
        metadataDF.as("metadata"),
        $"antenna.id" === $"metadata.id"
      ).drop($"metadata.id")
  }


  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter($"metric" === lit("devices_count"))
      .select($"timestamp", $"location", $"value")
      .withWatermark("timestamp", "1 minute")
      .groupBy($"location", window($"timestamp", "5 minutes"))
      .agg(
        avg($"value").as("avg_devices_count"),
        max($"value").as("max_devices_count"),
        min($"value").as("min_devices_count")
      )
      .select($"location", $"window.start".as("date"), $"avg_devices_count", $"max_devices_count", $"min_devices_count")
      //location TEXT, date TIMESTAMP, avg_devices_count BIGINT, max_devices_count BIGINT, min_devices_count BIGINT
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }
      .start()
      .awaitTermination()
  }


  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .select(
        $"timestamp", $"id", $"metric", $"value",
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour"),
      )
      .writeStream
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .partitionBy("year", "month", "day", "hour")
      .start
      .awaitTermination()
  }

  /**
   * Main for Streaming job
   *
   * @param args arguments for execution:
   *             kafkaServer topic jdbcUri jdbcMetadataTable aggJdbcTable jdbcUser jdbcPassword storagePath
   * Example:
   *   XXX.XXX.XXX.XXX:9092 antenna_telemetry jdbc:postgresql://XXX.XXX.XXX.XXXX:5432/postgres metadata antenna_agg postgres keepcoding /tmp/batch-storage
   */
  // Puedes descomentar este main y llamar con argumentos
  //def main(args: Array[String]): Unit = run(args)


  def main(args: Array[String]): Unit = {
    //run(args)
    val kafkaDF = readFromKafka("34.175.112.106:9092", "devices")
    val parsedDF = parserJsonData(kafkaDF)
    val storageFuture = writeToStorage(parsedDF, "/tmp/antenna_parquet/")
    val metadaDF = readAntennaMetadata(
      "jdbc:postgresql://34.173.65.17:5432/postgres",
      "metadata",
      "postgres",
      "dota"
    )
    val enrichDF = enrichAntennaWithMetadata(parsedDF, metadaDF)
    val countByLocation = computeDevicesCountByCoordinates(enrichDF)
    val jdbcFuture = writeToJdbc(countByLocation, "jdbc:postgresql://34.173.65.17:5432/postgres", "antenna_agg", "postgres", "keepcoding")

    Await.result(
      Future.sequence(Seq(storageFuture, jdbcFuture)), Duration.Inf
    )

    spark.close()
  }
}
