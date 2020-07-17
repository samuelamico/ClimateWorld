package observatory
import org.scalatest.{FlatSpec, Matchers}

class Mod2Spec extends FlatSpec with Matchers

class ExtractionTest extends FlatSpec with Matchers {
  //private val milestoneTest = namedMilestoneTest("data extraction", 1) _

  // Implement tests for the methods of the `Extraction` object
  val year = 1975
  val debug = true

  val stationsPath:String = "/stations.csv"
  val temperaturePath:String = s"/$year.csv"

  lazy val stations = Extraction.readStation(stationsPath).persist
  lazy val temperatures = Extraction.readTempYear(temperaturePath,year).persist
  lazy val joined = Extraction.joined(stations, temperatures).persist

  lazy val locateTemperatures = Extraction.locateTemperatures(year, stationsPath, temperaturePath)
  //print(locateTemperatures.take(10))


//ListConcat.concatelist("Hello",", ","World") shouldEqual("Hello, World")
  it should "Test LocateTemperature" in {
    assert(locateTemperatures.count(_._2==Location(70.933,-8.667)) === 363)
    assert(locateTemperatures.size === 2176493)
  }




}
