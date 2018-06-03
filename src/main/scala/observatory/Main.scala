package observatory

object Main extends App {
  
  override def main(args: Array[String]): Unit = {
    val year: Year = 2012
    val temperaturesFile: String = "/%s.csv".format(year)
    val stationsFile: String = "/stations.csv"
    
    val locateTemperatures = Extraction.locateTemperatures(year, stationsFile, temperaturesFile)
    
    Extraction.locationYearlyAverageRecords(locateTemperatures)
  }
}
