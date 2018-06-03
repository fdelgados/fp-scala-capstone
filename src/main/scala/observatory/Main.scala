package observatory

object Main extends App {
  
  override def main(args: Array[String]): Unit = {
    val year: Year = 1980
    val temperaturesFile: String = "/%s.csv".format(year)
    val stationsFile: String = "/stations.csv"

    val locateTemperatures = Extraction.locateTemperatures(year, stationsFile, temperaturesFile)

    Extraction.locationYearlyAverageRecords(locateTemperatures).take(100).foreach(println)
  }
}
