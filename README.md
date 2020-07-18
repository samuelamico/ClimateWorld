# Climate Temperature Variation


- [x] First Milestone - Extracting
- [ ] Second Milestone
- [ ] Third Milestone
- [ ] Fourth Milestone
- [ ] Fifth Milestone
- [ ] Last Milestone


*  **Data File**: The data you will use comes from the [National Center for Environmental Information of the United States](!https://www.ncei.noaa.gov/).
Our files use the comma-separated values format: each line contains an information record, which is itself made of several columns.

--------------------------------------------------------------


## First Milestone


* **Stations**
The stations.csv file contains one row per weather station, with the following columns:

 | STN identifier |	WBAN identifier |	Latitude	|  Longitude |

You might have noticed that there are two identifiers. Indeed, weather stations are uniquely identified by the compound key (STN, WBAN).Note that on some lines some columns might be empty. 

* **Temperatures**
The temperature files contain one row per day of the year, with the following columns:

STN identifier |	WBAN identifier |	Month |	Day	| Temperature (in degrees Fahrenheit)

The STN and WBAN identifiers refer to the weather station’s identifiers. The temperature field contains a decimal value (or 9999.9 if missing). The year number is given in the file name.

Again, all columns are not always provided for each line.


In this project, Temperature will always represent a (type Double) number of °C. 

```scala
type Temperature = Double // °C
type Year = Int
case class Location(lat: Double, lon: Double)
```

### First Function:

The signature for the first function:

```scala
def locateTemperatures(
  year: Year,
  stationsFile: String,
  temperaturesFile: String
): Iterable[(LocalDate, Location, Temperature)]
```

This method should return the list of all the temperature records converted in degrees Celsius along with their date and location (ignore data coming from stations that have no GPS coordinates). You should not round the temperature values. The file paths are resource paths, so they must be absolute locations in your classpath (so that you can read them with getResourceAsStream). For instance, the path for the resource file 1975.csv is /1975.csv, and loading it using scala.io.Source can be achieved as follows:

```scala
  val path = "/1975.csv"
  Source.fromInputStream(getClass.getResourceAsStream(path), "utf-8")
```

With the data given in the examples, this method would return the following sequence:

```scala
Seq(
  (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
  (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
  (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.0)
)
```

I have use both RDD and Dataframe approach, for example let's compare both Station functions:

```scala
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
```

In this function I read the input file from resource as stream and convert toStream, after this I just split the String, filter the not null values in the Array[String] and last I remove the wrong location and convert to a RDD case class object.

Using Dataframe is more simple and Optimizing (catalyst otimizer):

```scala
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
      //.format("csv")
      //.option("header", "false")
      //.option("delimiter", ",")
      .csv(resourcePath(stationFile))
      .withColumn("id", uniqueID(col("SIN"),col("WBAN")))
      .na.drop(Seq("Latitude","Longitude"))
      .withColumn("WBAN",extractDateAsOptionInt(col("WBAN")))
      .withColumn("SIN",extractDateAsOptionInt(col("SIN")))
      .withColumn("id", uniqueID(col("SIN"),col("WBAN")))
      .select(col("id"),col("Latitude"),col("Longitude"))
      .where(col("Latitude") =!= 0.0 && col("Longitude") =!= 0.0)

    stationDF
  }
```

First I read the csv path and create my own schema. Afte that I just read the csv file, rename the compund key to unique primary key id, and remove the wrong Latitude,Longitude and blank spaces in WBAN and SIN.

```scala
  val convert = udf((s: Double) => (s - 32.0) * (5.0/9.0))
  val uniqueID = udf((a: String,b: String) => a+b)
  val extractDateAsOptionInt = udf((d: String) => d match {
    case null => ""
    case s => s
  })
```

--------------------------------------------------------------

## Second Milestone

Your records contain the average temperature over a year, for each station’s location. Your work consists in building an image of 360×180 pixels, where each pixel shows the temperature at its location. The point at latitude 0 and longitude 0 (the intersection between the Greenwich meridian and the equator) will be at the center of the image:

![Location](https://github.com/samuelamico//image.jpg?raw=true)

In this figure, the red crosses represent the weather stations. As you can see, you will have to spatially interpolate the data in order to guess the temperature corresponding to the location of each pixel (such a pixel is represented by a green square in the picture). Then you will have to convert this temperature value into a pixel color based on a color scale:

### Spatial interpolation

```scala
def predictTemperature(
  temperatures: Iterable[(Location, Temperature)],
  location: Location
): Temperature
```

This method takes a sequence of known temperatures at the given locations, and a location where we want to guess the temperature, and returns an estimate based on the inverse distance weighting algorithm (you can use any p value greater or equal to 2; try and use whatever works best for you!). To approximate the distance between two locations, we suggest you to use the great-circle distance formula.

Note that the great-circle distance formula is known to have rounding errors for short distances (a few meters), but that’s not a problem for us because we don’t need such a high degree of precision. Thus, you can use the first formula given on the Wikipedia page, expanded to cover some edge cases like equal locations and antipodes

However, running the inverse distance weighting algorithm with small distances will result in huge numbers (since we divide by the distance raised to the power of p), which can be a problem. A solution to this problem is to directly use the known temperature of the close (less than 1 km) location as a prediction.

### Linear interpolation

```scala
def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color
```

This method takes a sequence of reference temperature values and their associated color, and a temperature value, and returns an estimate of the color corresponding to the given value, by applying a linear interpolation algorithm.

Note that the given points are not sorted in a particular order.

### Visualization

Once you have completed the above steps you can implement the visualize method to build an image (using the scrimage library) where each pixel shows the temperature corresponding to its location.

```scala
def visualize(
  temperatures: Iterable[(Location, Temperature)],
  colors: Iterable[(Temperature, Color)]
): Image
```