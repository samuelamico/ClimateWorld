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

