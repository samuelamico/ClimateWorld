package observatory

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types._
/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  // Create the Spark Session
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Climate Temperature")
    .config("spark.ui.port", "8081")
    .getOrCreate()
  import spark.implicits._

  // UDF functions
  val convert = udf((s: Double) => (s - 32.0) * (5.0/9.0))
  val uniqueID = udf((a: String,b: String) => a+b)
  val extractDateAsOptionInt = udf((d: String) => d match {
    case null => ""
    case s => s
  })

  // Reading File and Wrangling them
  def readStation(stationFile: String) = {
    // get path
    val path = getClass.getResource(stationFile).getPath
    // Schema for my dataframe
    val schemaStation = StructType(List(
      StructField("SIN",StringType,nullable = true),
      StructField("WBAN",StringType,nullable = true),
      StructField("Latitude",DoubleType,nullable = true),
      StructField("Longitude",DoubleType,nullable = true)
    ))

    // retrun the dataframe with unique ID
    val stationDF = spark.read.schema(schemaStation)
      .format("csv")
      //.option("header", "false")
      //.option("delimiter", ",")
      .load(path)
      .withColumn("id", uniqueID(col("SIN"),col("WBAN")))
      .na.drop(Seq("Latitude","Longitude"))
      .withColumn("WBAN",extractDateAsOptionInt(col("WBAN")))
      .withColumn("SIN",extractDateAsOptionInt(col("SIN")))
      .withColumn("id", uniqueID(col("SIN"),col("WBAN")))
      .select(col("id"),col("Latitude"),col("Longitude"))
      .where(col("Latitude") =!= 0.0 && col("Longitude") =!= 0.0)

    stationDF
  }

  def readTempYear(temperaturesFile: String,year: Int) = {
    // Read the spcefic file
    val file = "/" + year.toString() + ".csv"
    val tempPath = getClass.getResource(file).getPath

    // Struncture Schema
    val schemaTemp = StructType(List(
      StructField("SIN",StringType,nullable = true),
      StructField("WBAN",StringType,nullable = true),
      StructField("Month",IntegerType,nullable = true),
      StructField("Day",IntegerType,nullable = true),
      StructField("Temperature",DoubleType,nullable = true),
    ))

    val tempDF = spark.read.schema(schemaTemp)
      .format("csv")
      //.option("header", "false")
      //.option("delimiter", ",")
      .load(tempPath)
      .withColumn("Temp", convert(col("Temperature")))
      .withColumn("WBAN",extractDateAsOptionInt(col("WBAN")))
      .withColumn("SIN",extractDateAsOptionInt(col("SIN")))
      .withColumn("id_t", uniqueID(col("SIN"),col("WBAN")))
      .withColumn("year",lit(year))
      .select(col("id_t"),col("year"),col("Month"),col("Day"),col("Temp").as("Temperature"))
      .where(col("Temperature").between(-200.0,200.0))
    tempDF
  }


  def joined(station: Dataset[Row],temp: Dataset[Row]) = {
    temp.join(station,temp("id_t") === station("id"))
      .select(
        col("id"),
        col("year"),
        col("Month"),
        col("Day"),
        col("Temperature"),
        col("Latitude"),
        col("Longitude")
      )
      .as[Joined]
      .as[Joined]
      .map(j => (StationDate(j.day, j.month, j.year), Location(j.latitude, j.longitude), j.temperature))
      .toDF("date", "location", "temperature")
      .as[JoinedFormat]
  }


  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    **/

  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    // Reading Files:
    val stationDF = readStation(stationsFile)
    val tempDF = readTempYear(temperaturesFile,year)

    //result join
    val joinDS = joined(stationDF,tempDF)
    joinDS.collect()
      .par
      .map(
        jf => (jf.date.toLocalDate, jf.location, jf.temperature)
      ).seq

  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records
      .par
      .groupBy(_._2)
      .mapValues(
        l => l.foldLeft(0.0)(
          (t,r) => t + r._3) / l.size
      )
      .seq
  }

}
