package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

/**
  * 1st milestone: data extraction
  */
object Extraction {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  
  import org.apache.spark.sql._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._
  
  
  val spark: SparkSession =
    SparkSession
    .builder()
    .appName("Observatory")
    .config("spark.master", "local")
    .getOrCreate()
  
  import spark.implicits._
  
  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val temperaturesRdd = temperatures(year, spark.sparkContext.textFile(fsPath(temperaturesFile)))
    val stationsRdd = stations(spark.sparkContext.textFile(fsPath(stationsFile)))
    
    stationsRdd
      .join(temperaturesRdd)
      .map(record => (record._2._2.date, record._2._1, record._2._2.temperature))
      .collect()
      .toSeq
  }
  
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString
  
  private def temperatures(year: Year, temperatures: RDD[String]): RDD[(String, TemperatureRecord)] = {
    
    def hasCompleteRecord(line: String): Boolean = {
      val values = parseLine(line)
      
      values(2) != "" && values(3) != "" && values(4) == ""
    }
    
    def temperatureRecordFromLine(line: String): TemperatureRecord = {
      val values = parseLine(line)
    
      TemperatureRecord(
        LocalDate.of(year, values(2).toInt, values(3).toInt),
        (values(4).toDouble - 32) / 1.8
      )
    }
    
    temperatures
      .filter(line => hasCompleteRecord(line))
      .map(line => (buildIdentifier(line), temperatureRecordFromLine(line)))
      .cache()
  }
  
  /**
    * Builds a pair RDD with a string as key and a Location as the value
    *
    * @param stations An RDD whose elements are lines from stations.csv file
    *
    * @return A pair RDD with a string as key and a Location as the value
    */
  private def stations(stations: RDD[String]): RDD[(String, Location)] = {
    
    def hasCoordinates(line: String) : Boolean = {
      val values = parseLine(line)
      val latitude = values(2)
      val longitude = values(3)
  
      (latitude != "" && longitude != "") && (latitude.toDouble != 0.0 || longitude.toDouble != 0.0)
    }
    
    def locationFromLine(line: String): Location = {
      val values = parseLine(line)
      
      Location(values(2).toDouble, values(3).toDouble)
    }
    
    stations
      .filter(hasCoordinates)
      .map(line => (buildIdentifier(line), locationFromLine(line)))
      .cache()
  }
  
  /**
    * Builds an identifier from a CSV file line
    *
    * @param line CSV line
    *
    * @return A string representing an unique station identifier
    */
  private def buildIdentifier(line: String): String = {
    val values = parseLine(line)
    
    
    if (values(0) == "") values(1)
    else if (values(1) == "") values(0)
    else "%s%s".format(values(0), values(1))
  }
  
  private def parseLine(line: String): Array[String] = {
    line.split(",", -1).map(_.trim)
  }
  
  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    val recordsRDD = spark.sparkContext.parallelize(records.toSeq)
      .map(record => (record._2, record._3))
      .map(row)
    
    val schema = StructType(
      List(
        StructField("latitude", DoubleType, nullable = false),
        StructField("longitude", DoubleType, nullable = false),
        StructField("temperature", DoubleType, nullable = false)
      )
    )
   
    val dataFrame = spark.createDataFrame(recordsRDD, schema)
    
    dataFrame
      .groupBy($"latitude", $"longitude")
      .agg(avg($"temperature").as("avg_temperature"))
    
    val dataSet = dataFrame
      .map(r => (Location(r.getDouble(0), r.getDouble(1)), r.getDouble(2)))
    
    dataSet.collect()
  }
  
  def dfSchema(): StructType = {
    StructType(
      List(
        StructField("latitude", DoubleType, nullable = false),
        StructField("longitude", DoubleType, nullable = false),
        StructField("temperature", DoubleType, nullable = false)
      )
    )
  }
  
  def row(record: (Location, Temperature)): Row = {
    Row(record._1.lat, record._1.lon, record._2)
  }

}
