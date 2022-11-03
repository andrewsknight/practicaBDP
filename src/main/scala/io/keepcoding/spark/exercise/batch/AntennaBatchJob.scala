package io.keepcoding.spark.exercise.batch

import org.apache.spark.sql.functions.{array_distinct, col, column, dayofmonth, hour, lit, month, sum, window, year}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.json4s.scalap.scalasig.ScalaSigEntryParsers.literal
import sun.misc.MessageUtils.where

import java.time.OffsetDateTime

object AntennaBatchJob extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Final Exercise SQL Batch")
    .getOrCreate()

  import spark.implicits._



  override def readFromStorage(storageRootPath: String, offsetDate: OffsetDateTime): DataFrame = {

    spark
      .read
      .format("parquet")
      .load(s"${storageRootPath}/data")
      .filter(
        $"year" === offsetDate.getYear &&
          $"month" === offsetDate.getMonthValue &&
          $"day" === offsetDate.getDayOfMonth &&
          $"hour" === offsetDate.getHour
      )
      .cache()
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


  override def calculateUserQuotaLimit(userTotalBytesDF: DataFrame, userMetadataDF: DataFrame): DataFrame = {
    userTotalBytesDF.as("user")
      .select($"id", $"value", $"timestamp")
      .join(
        userMetadataDF.as("userMetadata")
          .select($"id", $"email", $"quota"),
        $"user.id" === $"userMetadata.id" && $"user.value" > $"userMetadata.quota"
      )
      .select($"userMetadata.email", $"user.value".as("usage"), $"userMetadata.quota", $"user.timestamp")

  }


  override def computeBytesByDate(dataFrame: DataFrame, key: String, literal: String, offsetDate: OffsetDateTime): DataFrame = {

    val alias = "id"
    val idKey = if (col(key) == $"id") col(key) else col(key).as(alias)
    dataFrame
      .select(idKey, $"bytes")
      .groupBy($"id")
      .agg(sum("bytes").as("value"))
      .withColumn("type", lit(literal))
      .withColumn("timestamp", lit(offsetDate.toEpochSecond).cast(TimestampType))


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
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day", "hour")

  }



  def main(args: Array[String]): Unit = {
    val jdbcUri = "jdbc:postgresql://34.175.167.179:5432/postgres"
    val jdbcUserTable = "user_metadata"
    val jdbcUser = "postgres"
    val jdbcPassword = "dota"
    val storageRootPath = "/tmp/datos/antenna_parquet/"

    val offsetDateTime = OffsetDateTime.parse("2022-11-02T17:00:00Z")

    val antennaDF = readFromStorage(storageRootPath, offsetDateTime)
    val userMetadataDF = readUserMetadata(jdbcUri, jdbcUserTable, jdbcUser, jdbcPassword)



    val userTotalBytesDF = computeBytesByDate(antennaDF, "id", "user_type_total", offsetDateTime).cache()
    val antennaTotalBytesDF = computeBytesByDate(antennaDF, "antenna_id", "antenna_total_bytes", offsetDateTime)
    val appTotalBytesDF = computeBytesByDate(antennaDF, "app", "app_total_bytes", offsetDateTime)
    val userQuotaLimit = calculateUserQuotaLimit(userTotalBytesDF, userMetadataDF)

    //writeToJdbc((userMetadataDF, jdbcUri,"user_medatada", jdbcUser, jdbcPassword))
    writeToJdbc(userTotalBytesDF, jdbcUri, "hourly_table", jdbcUser, jdbcPassword)
    writeToJdbc(antennaTotalBytesDF, jdbcUri, "hourly_table", jdbcUser, jdbcPassword)
    writeToJdbc(appTotalBytesDF, jdbcUri, "hourly_table", jdbcUser, jdbcPassword)
    writeToJdbc(userQuotaLimit, jdbcUri, "quota_table", jdbcUser, jdbcPassword)

    writeToStorage(antennaDF, storageRootPath)

    userQuotaLimit
      .show()

    spark.close()
  }

}
