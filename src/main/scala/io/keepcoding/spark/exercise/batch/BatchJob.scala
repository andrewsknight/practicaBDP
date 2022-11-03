package io.keepcoding.spark.exercise.batch

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession
  def readFromStorage(storageRootPath: String, offsetDate: OffsetDateTime): DataFrame
  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame
  def calculateUserQuotaLimit(userTotalBytesDF: DataFrame, userMetadataDF: DataFrame): DataFrame
  def computeBytesByDate(dataFrame: DataFrame, key: String, literal: String, offsetDate: OffsetDateTime): DataFrame
  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit
  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(offsetDateTime, storageRootPath, jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword, literal,hourly_table,quota_table) = args
    println(s"Running with: ${args.toSeq}")

    val key: String = ???

    val antennaDF = readFromStorage(storageRootPath, OffsetDateTime.parse(offsetDateTime))
    val userMetadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)

    val userTotalBytesDF = computeBytesByDate(antennaDF,  key,  literal,  OffsetDateTime.parse(offsetDateTime)).cache()

    val antennaTotalBytesDF = computeBytesByDate(antennaDF,  key,  literal,  OffsetDateTime.parse(offsetDateTime))
    val appTotalBytesDF = computeBytesByDate(antennaDF,  key,  literal, OffsetDateTime.parse( offsetDateTime))

    val userQuotaLimit = calculateUserQuotaLimit(userTotalBytesDF,userMetadataDF)

    writeToJdbc(userTotalBytesDF, jdbcUri, hourly_table, jdbcUser, jdbcPassword)
    writeToJdbc(antennaTotalBytesDF, jdbcUri, hourly_table,jdbcUser, jdbcPassword)
    writeToJdbc(appTotalBytesDF, jdbcUri, hourly_table,jdbcUser, jdbcPassword)
    writeToJdbc(userQuotaLimit, jdbcUri, quota_table, jdbcUser, jdbcPassword)

    writeToStorage(antennaDF, storageRootPath)

    spark.close()
  }

}
