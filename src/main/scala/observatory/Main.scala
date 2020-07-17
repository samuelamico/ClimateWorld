//package observatory.Extraction

import java.time.LocalDate

import observatory.Location
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types._
//import observatory.Extraction
case class Joined(id: String, year: Int, month: Int, day: Int, temperature: Double ,latitude:Double, longitude: Double)
case class StationDate(day: Int, month: Int, year: Int){
  def toLocalDate = LocalDate.of(year, month, day)
}

case class JoinedFormat(date: StationDate, location: Location, temperature: Double)

object Main extends App {


  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL basic example")
    .config("spark.ui.port", "8081")
    .getOrCreate()

  import spark.implicits._

  val convert = udf((s: Double) => (s - 32.0) * (5.0/9.0))
  val uniqueID = udf((a: String,b: String) => a+b )
  val extractDateAsOptionInt = udf((d: String) => d match {
    case null => ""
    case s => s
  })

  /// Using spark
  // Station
  val path = getClass.getResource("/stations.csv").getPath
  val schemaStation = StructType(List(
    StructField("SIN",StringType,nullable = true),
    StructField("WBAN",StringType,nullable = true),
    StructField("Latitude",DoubleType,nullable = true),
    StructField("Longitude",DoubleType,nullable = true)
  ))

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

  // Year
  val year = 1975
  val file = "/" + year.toString() + ".csv"
  val tempPath = getClass.getResource(file).getPath

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

  //tempDF.show()
  //stationDF.show()

  tempDF.join(stationDF,tempDF("id_t") === stationDF("id"))
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
    .show()




}
