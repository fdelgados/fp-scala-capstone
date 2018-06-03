package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}

import scala.reflect.ClassTag

/**
  * 1st milestone: data extraction
  */
object Extraction {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  
  
  val spark: SparkSession =
    SparkSession
    .builder()
    .appName("Observatory")
    .config("spark.master", "local")
    .getOrCreate()
  
  import spark.implicits._
  
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)
  
  implicit def tuple3[A1, A2, A3](implicit e1: Encoder[A1], e2: Encoder[A2], e3: Encoder[A3]): Encoder[(A1,A2,A3)] = {
    Encoders.tuple[A1,A2,A3](e1, e2, e3)
  }
  
  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val temperaturesDs: Dataset[TemperatureRecord] = temperatures(fsPath(temperaturesFile))
    val stationsDs: Dataset[Station] = stations(fsPath(stationsFile))
  
    def fahrenheitToCelsius(fahrenheit: Temperature): Double = {
      (fahrenheit - 32) / 1.8
    }
    
    stationsDs
      .join(temperaturesDs,
        stationsDs("stnId").eqNullSafe(temperaturesDs("stnId")) &&
          stationsDs("wbanId").eqNullSafe(temperaturesDs("wbanId"))
      )
      .map(row => (
        LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
        Location(row.getAs[Double]("latitude"), row.getAs[Double]("longitude")),
        fahrenheitToCelsius(row.getAs[Double]("temperature"))
      ))
      .collect()
  }
  
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString
  
  private def temperatures(temperaturesFile: String): Dataset[TemperatureRecord] = {
  
    val schema: StructType = StructType(
      List(
        StructField("stnId", IntegerType, nullable = true),
        StructField("wbanId", IntegerType, nullable = true),
        StructField("month", IntegerType, nullable = false),
        StructField("day", IntegerType, nullable = false),
        StructField("temperature", DoubleType, nullable = false)
      )
    )
    
    spark.read.option("header", value = false)
      .schema(schema)
      .csv(temperaturesFile).as[TemperatureRecord]
      .filter((record:TemperatureRecord) => record.temperature != 9999.9)
      .cache()
  }
  
  private def stations(stationsFile: String): Dataset[Station] = {
  
    val schema: StructType = StructType(
      List(
        StructField("stnId", IntegerType, nullable = true),
        StructField("wbanId", IntegerType, nullable = true),
        StructField("latitude", DoubleType, nullable = true),
        StructField("longitude", DoubleType, nullable = true)
      )
    )
    
    spark.read.option("header", value = false)
      .schema(schema)
      .csv(stationsFile).as[Station]
      .filter((station: Station) => station.latitude.isDefined && station.longitude.isDefined)
      .cache()
  }
  
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    spark.sparkContext.parallelize(records.toSeq)
      .map(record => (record._2, record._3))
      .toDF("location", "temperature")
      .groupBy($"location")
      .agg($"location", avg($"temperature").as("avg_temperature"))
      .select($"location".as[Location], $"avg_temperature".as[Double])
      .collect()
  }
}
