package qassignment

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, count, date_format, udf, lit, lower, to_date, when, lag, max, min}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Assignment extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  // Path for Input csv files
  val passengerFilePath = "./resources/passengers.csv"
  val flightFilePath = "./resources/flightData.csv"

  // Path for Output files
  val outputFilePath = "./target"

  // Get dataframes
  val passengerDf = getPassengerDf(passengerFilePath)
  passengerDf.show()

  val flightDf = getFlightDf(flightFilePath)
  flightDf.show()

  // Question 1: Output
  val question1 = getFlightsPerMonth(flightDf)
  question1.show(false)

  question1
    .coalesce(1)
    .write.option("header", "true")
    .csv(outputFilePath + "/Q1")

  // Question 2 : Output

  val question2 = getFrequentFlyers(flightDf, passengerDf, 100)
  question2.show(false)

  question2
    .coalesce(1)
    .write.option("header", "true")
    .csv(outputFilePath + "/Q2")

  // Question 3 : Output

  val question3 = getLongestRun(flightDf, "uk")
  question3.show(false)

  question3
    .coalesce(1)
    .write.option("header","true")
    .csv(outputFilePath + "/Q3")

  // Question 4 : Output
  val question4 = getflightsTogether(flightDf, 3)
  question4.show(false)

  question4
    .coalesce(1)
    .write.option("header","true")
    .csv(outputFilePath + "/Q4")

  // Question 5 : Output
  val question5 = getflightsTogetherByDate(flightDf, 3, "2017-01-01", "2017-05-31")
  question5.show(false)

  question5
    .coalesce(1)
    .write.option("header","true")
    .csv(outputFilePath + "/Q5")

  // Get Passengers data

  def getPassengerDf(passengerFilePath: String): DataFrame = {

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val passengerDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(passengerFilePath)

    return passengerDf
  }

  // Get Flights Data

  def getFlightDf(flightFilePath: String): DataFrame = {

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val flightDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(flightFilePath)

    return flightDf

  }

  // Question 1: Flights per month

  def getFlightsPerMonth(flightDf: DataFrame): DataFrame = {

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Group by month and count flights
    val flightsPerMonth = flightDf
      .withColumn("Month", date_format(col("date"), "MM"))
      .groupBy("Month")
      .agg(countDistinct("flightId").alias("Number of Flights"))
      .orderBy("Month")

    return flightsPerMonth
  }

  // Question 2: Get 100 most frequent flyers

  def getFrequentFlyers(flightDf: DataFrame, passengerDf: DataFrame, topN: Int = 100): DataFrame = {

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val flightsPerPassenger = flightDf
      .groupBy("passengerId")
      .agg(countDistinct("flightId").alias("Number of Flights"))
      .orderBy(desc("Number of Flights"))
      .limit(topN)

    // Join with passengers to get their Names
    val topFlyers = flightsPerPassenger
      .join(passengerDf, Seq("passengerId"), "inner")
      .withColumnRenamed("passengerId","Passenger Id")
      .withColumnRenamed("firstName","First Name")
      .withColumnRenamed("lastName","Last Name")
      .orderBy(desc("Number of Flights"))

    return topFlyers
  }

  // Question 3: Get the largest number of countries a passenger has been apart from UK

  def getLongestRun(flightDf: DataFrame, countryCode: String = "uk"): DataFrame = {

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Add new column with 1 or 2 if the passenger either travels from or to UK
    val getStartnArriveUK = flightDf
      .withColumn("starts_arrives_uk",
        when(
          lower(col("from")).equalTo(countryCode),
          lit(1)
        ).when(
          lower(col("to")).equalTo(countryCode),
          lit(2)
        ).otherwise(0)
      )
      .orderBy(col("date").desc)

    //    getStartnArriveUK.show()
    //    val Full = getStartnArriveUK.count()

    // Get records of travel that dont have UK as location
    val nonUKdata = getStartnArriveUK
      .filter(getStartnArriveUK("starts_arrives_uk") === 0)

    //    val UKdata = getStartnArriveUK
    //      .filter(getStartnArriveUK("starts_arrives_uk") > 0)
    //        nonUKdata.show()
    //    val nonUK = nonUKdata.count()

    // Create a list of from and to locations
    val dataWithCountries = nonUKdata
      .groupBy("passengerId")
      .agg(concat(collect_list(col("from")), collect_list(col("to"))).name("countries"))

    // Count the distinct countries in the list
    val passengerLongestRuns = dataWithCountries
      .withColumn("Longest_run", size(array_distinct(col("countries"))))
      .orderBy(col("Longest_run").desc)

    val LongestRun = passengerLongestRuns
      .drop("countries")
      .withColumnRenamed("passengerID","Passenger ID")

    return LongestRun
  }

  // Question 4 : Passengers who have been on more than 3 flights together

  def getflightsTogether(flightDf: DataFrame, minSharedFlights: Int = 3): DataFrame = {

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Self Join the Flights data on flight Id
    val flightselfjoinDf = flightDf.as("flightdata1")
      .join(flightDf.as("flightdata2"),
        col("flightdata1.passengerId") < col("flightdata2.passengerId") &&
          col("flightdata1.flightId") === col("flightdata2.flightId") &&
          col("flightdata1.date") === col("flightdata2.date"),
        "inner")
      .select(col("flightdata1.passengerId"), col("flightdata2.passengerId").as("passengerId2"), col("flightdata1.flightId"), col("flightdata1.date"))

    //    flightselfjoinDf.show()

    // Filter and order by shared flights
    val flightsTogether = flightselfjoinDf
      .groupBy("passengerId", "passengerId2")
      .agg(countDistinct("flightdata1.flightId").alias("Number of flights together"), min("date") as "From", max("date") as "To")
      .filter(col("Number of flights together") >= minSharedFlights && col("passengerId").notEqual(col("passengerId2")))
      .withColumnRenamed("passengerId","Passenger Id 1")
      .withColumnRenamed("passengerId2","Passenger Id 2")
      .orderBy(desc("Number of flights together"))

    return flightsTogether

  }

  // Question 5 : Passengers who have been on more than N flights together within a timeframe

  def getflightsTogetherByDate(flightDf: DataFrame, minSharedFlights: Int = 3, startDate: String, endDate: String): DataFrame = {

    // Filter the raw data based on date range
    val flightsByDate = flightDf
      .filter(col("date").geq(startDate) && col("date").leq(endDate))

    // Use getflightsTogether function with modified dataframe
    getflightsTogether(flightsByDate,minSharedFlights)

  }
}