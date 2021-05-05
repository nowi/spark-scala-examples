package com.sparkbyexamples.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

object SparkPostgresTest extends App{
  val jdbcHostname = "localhost"
  val jdbcPort = 5432
  val jdbcUsername = "postgres"
  val jdbcPassword = "password"
  val jdbcDatabase = "postgres"

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate();

  spark.sparkContext.setLogLevel("ERROR")


  val sparkContext:SparkContext = spark.sparkContext
  val sqlCon:SQLContext = spark.sqlContext

  val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)

  // Create the JDBC URL without passing in the user and password parameters.
  val jdbcUrl = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
  // Create a Properties() object to hold the parameters.
  import java.util.Properties

  val connectionProperties = new Properties()
  connectionProperties.put("user", s"${jdbcUsername}")
  connectionProperties.put("password", s"${jdbcPassword}")

  Class.forName("org.postgresql.Driver")

  import java.sql.DriverManager
  val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
  connection.isClosed()


  val meetings_table = spark.read.jdbc(jdbcUrl, "zoom._airbyte_raw_meetings", connectionProperties)

  val meetings_attendance_table = spark.read.jdbc(jdbcUrl, "zoom._airbyte_meetings_attendance", connectionProperties)

  val organizerToMeetings = meetings_table.groupBy("hostId")

  val organizerToTotalMeetingTime = organizerToMeetings.sum("duration")

  val attendeeToMeeting = meetings_attendance_table.groupBy("hostId")



}
