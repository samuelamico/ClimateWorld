package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.io._
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

  // Read File Path
  def getDSFromResource(resource: String) = {
    val fileStream = Source.getClass.getResourceAsStream(resource)
    spark.sparkContext.makeRDD(Source.fromInputStream(fileStream).getLines().toList).toDF
  }

  // UDF functions
  val convert = udf((s: Double) => (s - 32.0) * (5.0/9.0))
  val uniqueID = udf((a: String,b: String) => a+b)
  val extractDateAsOptionInt = udf((d: String) => d match {
    case null => ""
    case s => s
  })
  def convertCelsius(fahrenheit: Temperature): Temperature =
    (fahrenheit - 32) / 1.8
  private def resourcePath(resource: String): String = Paths.get(getClass.getResource(resource).toURI).toString


  // Reading File and Wrangling them
  def readStation(stationFile: String) = {
    val stationsRdd = spark.sparkContext.parallelize(
      Source.fromInputStream(getClass.getResourceAsStream(stationFile), "utf-8").getLines().toStream
    )

    val stations = stationsRdd
      .map(_.split(','))
      .filter(_.length == 4)
      .filter(line => line(2).toDouble != 0.0 && line(3).toDouble != 0.0)
      .map(a => ((a(0), a(1)), Location(a(2).toDouble, a(3).toDouble)))

    stations
  }

  def readTempYear(temperaturesFile: String,year: Int) = {
    val tempRdd = spark.sparkContext.parallelize(
      Source.fromInputStream(getClass.getResourceAsStream(temperaturesFile), "utf-8").getLines().toStream
    )

    tempRdd.map(_.split(','))
      .filter(_.length == 5)
      .map(a => ((a(0), a(1)), (LocalDate.of(year, a(2).toInt, a(3).toInt), convertCelsius(a(4).toDouble))))

  }


  def joined(station: RDD[((String,String),Location)],temp: RDD[((String,String),(LocalDate,Double))]) = {
    station.join(temp).mapValues(v => (v._2._1, v._1, v._2._2)).values
  }


  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    **/

  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    // Reading Files:
    val stationRdd = readStation(stationsFile)
    val tempRdd = readTempYear(temperaturesFile,year)
    joined(stationRdd,tempRdd).collect().toSeq

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
