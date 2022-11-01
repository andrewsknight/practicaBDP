package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AntennaBatchJob extends BatchJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Final Exercise SQL Batch")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}/data")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
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
      .groupBy($"location", window($"timestamp", "1 hour"))
      .agg(
        avg($"value").as("avg_devices_count"),
        max($"value").as("max_devices_count"),
        min($"value").as("min_devices_count")
      )
      .select($"location", $"window.start".as("date"), $"avg_devices_count", $"max_devices_count", $"min_devices_count")
  }

  override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter($"metric" === lit("status") and $"value" === lit(-1))
      .select($"timestamp", $"model", $"version", $"id")
      .groupBy(window($"timestamp", "1 hour"), $"model", $"version")
      .agg(approx_count_distinct($"id").as("antennas_num"))
      .select($"antennas_num", $"window.start".as("date"), $"model", $"version")
  }

  override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = {
    val stateCountsDF = dataFrame
      .filter($"metric" === lit("status"))
      .withColumn("state",
        when($"value" === lit(0), "disable")
          .when($"value" === lit(1), "enable")
          .otherwise("error")
      )
      .select($"timestamp", $"id", $"state")
      .groupBy(window($"timestamp", "1 hour"), $"id")
      .pivot($"state", Seq("enable", "disable", "error"))
      .agg(count("*"))
      .select($"window.start".as("date"), $"id", $"enable", $"disable", $"error")
      .as("state")
      .cache()

    val countsDF = dataFrame
      .filter($"metric" === lit("status"))
      .select($"timestamp", $"id")
      .groupBy(window($"timestamp", "1 hour"), $"id")
      .agg(count("*").as("total_count"))
      .select($"window.start".as("date"), $"id", $"total_count")
      .as("global")
      .cache()

    stateCountsDF
      .join(
        countsDF,
        $"state.id" === $"global.id" and $"state.date" === $"global.date"
      )
      .select($"state.date".as("date"),
        $"state.id".as("id"),
        ($"enable".cast(DoubleType) / $"total_count" * 100).as("enable"),
        ($"disable".cast(DoubleType) / $"total_count" * 100).as("disable"),
        ($"error".cast(DoubleType) / $"total_count" * 100).as("error"))
      .na.fill(0.0, Seq("enable", "disable", "error"))
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
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

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath}/historical")
  }

  /**
   * Main for Batch job
   *
   * @param args arguments for execution:
   *             filterDate storagePath jdbcUri jdbcMetadataTable aggJdbcTable aggJdbcErrorTable aggJdbcPercentTable jdbcUser jdbcPassword
   * Example:
   *   2007-01-23T10:15:30Z /tmp/batch-storage jdbc:postgresql://XXX.XXX.XXX.XXXX:5432/postgres metadata antenna_1h_agg antenna_errors_agg antenna_percent_agg postgres keepcoding
   */
  // puedes descomentar este main para pasar parametros a la ejecucion
  //def main(args: Array[String]): Unit = run(args)

  def main(args: Array[String]): Unit = {
    val jdbcUri = "jdbc:postgresql://34.173.65.17:5432/postgres"
    val jdbcUser = "postgres"
    val jdbcPassword = "keepcoding"

    val offsetDateTime = OffsetDateTime.parse("2022-10-20T23:00:00Z")
    val parquetDF = readFromStorage("/tmp/antenna_parquet/", offsetDateTime)
    val metadataDF = readAntennaMetadata(jdbcUri, "metadata", jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichAntennaWithMetadata(parquetDF, metadataDF).cache()

    val aggByCoordinatesDF = computeDevicesCountByCoordinates(antennaMetadataDF)
    val aggPercentStatusDF = computePercentStatusByID(antennaMetadataDF)
    val aggErroAntennaDF = computeErrorAntennaByModelAndVersion(antennaMetadataDF)

    writeToJdbc(aggByCoordinatesDF, jdbcUri, "antenna_1h_agg", jdbcUser, jdbcPassword)
    writeToJdbc(aggPercentStatusDF, jdbcUri, "antenna_errors_agg", jdbcUser, jdbcPassword)
    writeToJdbc(aggErroAntennaDF, jdbcUri, "antenna_percent_agg", jdbcUser, jdbcPassword)
    writeToStorage(parquetDF, "/tmp/antenna_parquet/")

    spark.close()
  }

}
