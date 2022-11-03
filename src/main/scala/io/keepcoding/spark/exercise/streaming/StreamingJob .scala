package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

case class AntennaMessage(timestamp: Timestamp, id: String, metric: String, value: Long)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeBytesBy(dataFrame: DataFrame, key: Column, literal: String): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]



  def run(args: Array[String]): Unit = {
    val Array(kafkaServer:String, topic:String, jdbcUri:String, jdbcMetadataTable:String, aggJdbcTable:String, jdbcUser:String, jdbcPassword:String, storagePath:String,alias:String, literal:String) = args
    println(s"Running with: ${args.toSeq}")
    var key: Column = ???
    val kafkaDF = readFromKafka(kafkaServer, topic)
    val antennaDF = parserJsonData(kafkaDF)
    val metadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichAntennaWithMetadata(antennaDF, metadataDF)
    val storageFuture = writeToStorage(antennaDF, storagePath)

    val bytesByAntenna = computeBytesBy(antennaMetadataDF, key,  literal)
    val bytesByUser = computeBytesBy(antennaMetadataDF, key,  literal)
    val bytesByApp = computeBytesBy(antennaMetadataDF, key,  literal)

    val jdbcFutureBytesByAntenna = writeToJdbc(bytesByAntenna, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val jdbcFutureBytesByUser = writeToJdbc(bytesByUser, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val jdbcFutureBytesByApp = writeToJdbc(bytesByApp, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(jdbcFutureBytesByAntenna, jdbcFutureBytesByUser,jdbcFutureBytesByApp, storageFuture)), Duration.Inf)

    spark.close()
  }

}
