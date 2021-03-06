# Climate Temperature Variation


- [x] First Milestone - Extracting
- [x] Second Milestone
- [X] Third Milestone
- [X] Fourth Milestone
- [X] Fifth Milestone
- [X] Last Milestone


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

![Location](https://github.com/samuelamico/ClimateWorld/blob/master/img/milestone2.PNG)

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

Note that the (x,y) coordinates of the top-left pixel is (0,0) and then the x axis grows to the right and the y axis grows to the bottom, whereas the latitude and longitude origin, (0,0), is at the center of the image, and the top-left pixel has GPS coordinates (90, -180).


## Third Milestone

n Web based mapping applications, the whole map is broken down into small images of size 256×256 pixels, called tiles. Each tile shows a part of the map at a given location and zoom level. Your work consists in producing these tiles using the Web Mercator projection.

You can monitor your progress by submitting your work at any time during the development of this milestone. Your submission token and the list of your graded submissions is available on this page.

### Tile generation

```scala
def tileLocation(tile: Tile): Location
```

This method converts a tile's geographic position to its corresponding GPS coordinates, by applying the Web Mercator projection.

```scala
def tile(
  temperatures: Iterable[(Location, Temperature)],
  colors: Iterable[(Temperature, Color)],
  tile: Tile
): Image
```

This method returns a 256×256 image showing the given temperatures, using the given color scale, at the location corresponding to the given zoom, x and y values. Note that the pixels of the image must be a little bit transparent so that when we will overlay the tile on the map, the map will still be visible. We recommend using an alpha value of 127.

```scala
def generateTiles[Data](
  yearlyData: Iterable[(Year, Data)],
  generateImage: (Year, Tile, Data) => Unit
): Unit
```

This method generates all the tiles for a given dataset yearlyData, for zoom levels 0 to 3 (included). The dataset contains pairs of (Year, Data) values, or, said otherwise, data associated with years. In your case, this data will be the result of Extraction.locationYearlyAverageRecords. The second parameter of the generateTiles method is a function that takes a year, the coordinates of the tile to generate, and the data associated with the year, and computes the tile and writes it on your filesystem.



## Fourth Milestone

Computing deviations means comparing a value to a previous value which serves as a reference, or a “normal” temperature. You will first compute the average temperatures all over the world between 1975 and 1990. This will constitute your reference temperatures, which we refer to as “normals”. You will then compare the yearly average temperatures, for each year between 1991 and 2015, to the normals.

In order to make things faster, you will first spatially interpolate your scattered data into a regular grid:

### Grid generation

To describe a grid point's location, we'll use integer latitude and longitude values. This way, every grid point (in green above) is the intersection of a circle of latitude and a line of longitude. Since this is a new coordinate system, we're introducing another case class, quite similar to Location but with integer coordinates:

```scala
case class GridLocation(lat: Int, lon: Int)
```

The latitude can be any integer between -89 and 90, and the longitude can be any integer between -180 and 179. The top-left corner has coordinates (90, -180), and the bottom-right corner has coordinates (-89, 179).

The grid associates every grid location with a temperature. You are free to internally represent the grid as you want (e.g. using a class Grid), but to interoperate with the grading system you will have to convert it to a function of type GridLocation => Temperature, which returns the temperature at the given grid location.

```scala
def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature
```

It takes as parameter the temperatures associated with their location and returns the corresponding grid.

### Average and deviation computation

```scala
def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature
```

This method takes a sequence of temperature data over several years (each “temperature data” for one year being a sequence of pairs of average yearly temperature and location), and returns a grid containing the average temperature over the given years at each location.

```scala
def deviation(
  temperatures: Iterable[(Location, Temperature)],
  normals: GridLocation => Temperature
): GridLocation => Temperature
```

This method takes temperature data and a grid containing normal temperatures, and returns a grid containing temperature deviations from the normals.
