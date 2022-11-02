package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object ByteStreamingJob extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Practica BDP")
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
    val jsonSchema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("app", StringType, nullable = false)
    )
    )

    dataFrame
      .select(from_json($"value".cast(StringType), jsonSchema).as("json"))
      .select($"json.*")


  }

  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {

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
    antennaDF.as("a")
      .join(
        metadataDF.as("b"),
        $"a.id" === $"b.id"
      )
      .drop($"b.id")

  }

  override def computeBytesByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"antenna_id", $"bytes")
      .withWatermark("timestamp", "10 seconds")

      .groupBy($"antenna_id", window($"timestamp", "30 seconds"))
      .agg(sum("bytes").as("value"))
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"value")
      .withColumn("type", lit("antenna_bytes"))


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
        $"timestamp", $"id", $"antenna_id", $"bytes", $"app",
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )
      .writeStream
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .partitionBy("year", "month", "day", "hour")
      .start
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // definicion de variables
    val jdbcURI = "jdbc:postgresql://34.175.167.179:5432/postgres"
    val jdbcUserTable = "user_metadata"
    val jdbcUser = "postgres"
    val jdbcPassword = "dota"
    val kafkaServer = "34.175.206.84:9092"
    val kafkaTopic = "devices"
    val localStoragePath = "/tmp/datos/antenna_parquet/"
    val countByAntennaTable = "bytes"

    // kafka data
    val kafkaDF = readFromKafka(kafkaServer, kafkaTopic)
    val parsedDF = parserJsonData(kafkaDF)

    // escribe en disco
    val storageFuture = writeToStorage(parsedDF, localStoragePath)

    // lee la metadata de usuario
    val metadataDF = readUserMetadata(
      jdbcURI,
      jdbcUserTable,
      jdbcUser,
      jdbcPassword)

    val enrichDF = enrichAntennaWithMetadata(parsedDF, metadataDF)

    val bytesByAntenna = computeBytesByAntenna(enrichDF)
//    val jdbcFuture = writeToJdbc(bytesByAntenna, jdbcURI, countByAntennaTable, jdbcUser, jdbcPassword)


        bytesByAntenna
          .writeStream
          .format("console")
          .start()
          .awaitTermination()

    Await.result(
      Future.sequence(Seq(storageFuture/*, jdbcFuture*/)), Duration.Inf
    )

    spark.close()
  }
}
