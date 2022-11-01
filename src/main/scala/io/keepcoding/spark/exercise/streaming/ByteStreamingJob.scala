package io.keepcoding.spark.exercise.streaming
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.concurrent.Future

object ByteStreamingJob  extends StreamingJob {
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
      StructField("timestamp",TimestampType, nullable = false),
      StructField("id",StringType, nullable = false),
      StructField("antenna_id",StringType, nullable = false),
      StructField("bytes",LongType, nullable = false),
      StructField("app",StringType, nullable = false)
      )
    )

      dataFrame
        .select(from_json ($"value".cast(StringType), jsonSchema).as("json"))
        .select($"json.*")


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
    antennaDF.as("a")
      .join(
        metadataDF.as("b"),
      $"a.id" === $"b.id"
      )
      .drop($"b.id")

  }

  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = {
      dataFrame
        .select($"timestamp", $"antenna_id", $"bytes")
        .withWatermark("timestamp","10 seconds")
        .groupBy($"antenna_id", window($"timestamp", "30 seconds"))
        .agg(
        sum($"bytes").as("value")
        )
        .select($"window.start".as("date"), $"antenna_id".as("id"),$"value")
        .withColumn("type", lit("antenna_bytes"))



  }


  /*
    .agg( approx_count_distinct($"antenna_id").as("id"),
      sum($"bytes").as("value")
    )
    .select($"window.start".as("date"), $"id", $"value")

   */

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = ???

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = ???

  def main(args: Array[String]) : Unit = {
    //run(args)

      val kafkaDF = readFromKafka("34.175.206.84:9092", "devices")
      val parseDF = parserJsonData(kafkaDF)

    val metadataDF = readAntennaMetadata(
      "jdbc:postgresql://34.175.167.179:5432/postgres",
      "user_metadata",
          "postgres",
      "dota")



    val enrichDF = enrichAntennaWithMetadata(parseDF, metadataDF)

   val selectDelaTabla = computeDevicesCountByCoordinates(enrichDF)



    selectDelaTabla
      .writeStream
      .format("console")
      .start()
      .awaitTermination()


  }
}
