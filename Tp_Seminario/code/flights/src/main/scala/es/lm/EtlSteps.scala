package es.arjon

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

CREATE SCHEMA workshop;

/*case class Stock(name: String,
                 dateTime: String,
                 open: Double,
                 high: Double,
                 low: Double,
                 close: Double)
*/

case class flight(FechaHora: timestamp,
					Callsign: String,
					Matricula: String,
					Aeronave_Cod: String,
					Aerolinea_Cod: String,
					Aerolinea_Nombre: String,
					Origen: String,
					Destino: String)

/*
object Stock {
  def fromCSV(symbol: String, line: String): Option[Stock] = {
    val v = line.split(",")

    try {
      Some(
        Stock(
          symbol,
          dateTime = v(0),
          open = v(1).toDouble,
          high = v(2).toDouble,
          low = v(3).toDouble,
          close = v(4).toDouble
        )
      )

    } catch {
      case ex: Exception => {
        println(s"Failed to process $symbol, with input $line, with ${ex.toString}")
        None
      }
    }

  }
}
*/

object Flight {
  def fromTXT(Aerolinea_Cod: String, line: String): Option[Stock] = {	// ni idea que es line
    val v = line.split(";")												

    try {
      Some(
        Flight(
			FechaHora = v(0),			// ni idea cómo poner las cosas... toString??
			Callsign = v(1).toString,
			Matricula = v(2).toString,
			Aeronave_Cod = v(3).toString,
			Aerolinea_Cod = v(4).toString,
			Aerolinea_Nombre = v(5).toString,
			Origen = v(6).toString,
			Destino = v(7).toString
			)
      )

    } catch {
      case ex: Exception => {
        println(s"Failed to process $Aerolinea_Cod, with input $line, with ${ex.toString}")
        None
      }
    }

  }
}

/*
object RunAll {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: RunAll <dataset folder> <lookup file> <output folder>
           |  <dataset folder> folder where stocks data is located
           |  <lookup file> file containing lookup information
           |  <output folder> folder to write parquet data
           |
           |RunAll /dataset/stocks-small /dataset/yahoo-symbols-201709.csv /dataset/output.parquet
        """.stripMargin)
      System.exit(1)
    }

    val Array(stocksFolder, lookupSymbol, outputFolder) = args


    val spark = SparkSession.
      builder.
      appName("Stocks:ETL").
      getOrCreate()

    val stocksDS = ReadStockCSV.process(spark, stocksFolder)
    val lookup = ReadSymbolLookup.process(spark, lookupSymbol)

    // For implicit conversions like converting RDDs to DataFrames
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val ds = stocksDS.
      withColumn("full_date", unix_timestamp($"dateTime", "yyyy-MM-dd").cast("timestamp")).
      filter("full_date >= \"2017-09-01\"").
      withColumn("year", year($"full_date")).
      withColumn("month", month($"full_date")).
      withColumn("day", dayofmonth($"full_date")).
      drop($"dateTime").
      withColumnRenamed("name", "symbol").
      join(lookup, Seq("symbol"))

    // https://weishungchung.com/2016/08/21/spark-analyzing-stock-price/
    val movingAverageWindow20 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-20, 0)
    val movingAverageWindow50 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-50, 0)
    val movingAverageWindow100 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-100, 0)

    // Calculate the moving average
    val stocksMA = ds.
      withColumn("ma20", avg($"close").over(movingAverageWindow20)).
      withColumn("ma50", avg($"close").over(movingAverageWindow50)).
      withColumn("ma100", avg($"close").over(movingAverageWindow100))

    stocksMA.show(100)

    DatasetToParquet.process(spark, stocksMA, outputFolder)

    DatasetToPostgres.process(spark, stocksMA)

    spark.stop()
  }
}
*/
object RunAll {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: RunAll <dataset folder> <lookup file> <output folder>
           |  <dataset folder> folder where flights data is located
           |  <lookup file> file containing lookup information		
           |  <output folder> folder to write parquet data
           |
           |RunAll /dataset/flights /dataset/DB_Aux_Seminario_Intensivo.txt /dataset/output.parquet			// cambie los nombres a Flights y la base auxiliar
																											
		""".stripMargin)
      System.exit(1)
    }

    // val Array(stocksFolder, lookupSymbol, outputFolder) = args
	val Array(flightsFolder, lookupAerolinea, outputFolder) = args

    val spark = SparkSession.
      builder.
      // appName("Stocks:ETL").
      appName("Flights:ETL").
	  getOrCreate()

    //val stocksDS = ReadStockCSV.process(spark, stocksFolder)
    //val lookup = ReadSymbolLookup.process(spark, lookupSymbol)
	val flightsDS = ReadFlightsTXT.process(spark, flightsFolder)
	val lookup = ReadAerolineaLookup.process(spark, lookupAerolinea)
	
    // For implicit conversions like converting RDDs to DataFrames
    import org.apache.spark.sql.functions._
    import spark.implicits._

    /* val ds = stocksDS.
      withColumn("full_date", unix_timestamp($"dateTime", "yyyy-MM-dd").cast("timestamp")).
      filter("full_date >= \"2017-09-01\"").
      withColumn("year", year($"full_date")).
      withColumn("month", month($"full_date")).
      withColumn("day", dayofmonth($"full_date")).
      drop($"dateTime").
      withColumnRenamed("name", "symbol").
      join(lookup, Seq("symbol"))
	*/
	
	val ds = flightsDS.
      withColumn("FechaHora", unix_timestamp($"dateTime", "dd-MM-yyyy HH:mm").cast("timestamp")).
      filter("FechaHora" >= \"2014-12-31\"").		// agrego un filtro de tiempo para solo tener 2015 a 2017
      withColumn("Year", year($"full_date")).
      withColumn("Month", month($"full_date")).
      withColumn("Day", dayofmonth($"full_date")).
      drop($"dateTime").
      withColumnRenamed("Aerolinea_Nombre", "Aerolinea_Cod").
      join(lookup, Seq("Aerolinea_Cod"))
		
    
	// NO TENEMOS QUE HACER NINGUN CALCULO ADICIONAL SOBRE LOS DATOS
	// *************************************************************
	// entonces lo que llama stocksMA, que es el dataset original con las columnas de los MA agregadas, no va... queda el flightsDS original
	
	// https://weishungchung.com/2016/08/21/spark-analyzing-stock-price/
    // val movingAverageWindow20 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-20, 0)
    // val movingAverageWindow50 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-50, 0)
    // val movingAverageWindow100 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-100, 0)

    // Calculate the moving average
    //val stocksMA = ds.
    //  withColumn("ma20", avg($"close").over(movingAverageWindow20)).
    //  withColumn("ma50", avg($"close").over(movingAverageWindow50)).
    //  withColumn("ma100", avg($"close").over(movingAverageWindow100))

    flightsDS.show(100)

    DatasetToParquet.process(spark, flightsDS, outputFolder)

    DatasetToPostgres.process(spark, flightsDS)

    spark.stop()
  }
}
/*
object ReadStockCSV {
  def process(spark: SparkSession, originFolder: String) = {

    // Using SparkContext to use RDD
    val sc = spark.sparkContext
    val files = sc.wholeTextFiles(originFolder, minPartitions = 40)

    val stocks = files.map { case (filename, content) =>
      val symbol = new java.io.File(filename).
        getName.
        split('.')(0).
        toUpperCase

      content.split("\n").flatMap { line =>
        Stock.fromCSV(symbol, line)
      }
    }.
      flatMap(e => e).
      cache

    import spark.implicits._

    stocks.toDS.as[Stock]
  }
}
*/
object ReadFlightsTXT {
  def process(spark: SparkSession, originFolder: String) = {

    // Using SparkContext to use RDD
    val sc = spark.sparkContext
    val files = sc.wholeTextFiles(originFolder, minPartitions = 40)

    val flights = files.map { case (filename, content) =>
      val Aerolinea_Cod = new java.io.File(filename).
        getName.
        split('.')(0).
        toUpperCase

      content.split("\n").flatMap { line =>
        Flight.fromTXT(Aerolinea_Cod, line)			// NO SE QUE ES LINE!!!
      }
    }.
      flatMap(e => e).
      cache

    import spark.implicits._

    flights.toDS.as[Flight]
  }
}


/*object ReadSymbolLookup {
  def process(spark: SparkSession, file: String) = {
    import spark.implicits._
    spark.read.
      option("header", true).
      option("inferSchema", true).
      csv(file).
      select($"Ticker", $"Category Name").
      withColumnRenamed("Ticker", "symbol").
      withColumnRenamed("Category Name", "category")
  }
}
*/
object ReadAerolineaLookup {
  def process(spark: SparkSession, file: String) = {
    import spark.implicits._
    spark.read.
      option("header", true).
      option("inferSchema", true).
      txt(file).
      select($"Aerolinea_OACI", $"Aerolinea").
      withColumnRenamed("Aerolinea_OACI", "Aerolinea_Cod").
      withColumnRenamed("Aerolinea", "Aerolinea_Nombre")
  }
}

object DatasetToParquet {
  def process(spark: SparkSession, df: DataFrame, destinationFolder: String): Unit = {
    // https://stackoverflow.com/questions/43731679/how-to-save-a-partitioned-parquet-file-in-spark-2-1
    df.
      write.
      mode("overwrite").
      partitionBy("year", "month", "day").
      parquet(destinationFolder)
  }
}

object DatasetToPostgres {

  def process(spark: SparkSession, df: DataFrame): Unit = {
    // Write to Postgres
    val connectionProperties = new java.util.Properties
    connectionProperties.put("user", "workshop")
    connectionProperties.put("password", "w0rkzh0p")
    val jdbcUrl = s"jdbc:postgresql://postgres:5432/workshop"

    df.
      drop("year", "month", "day"). // drop unused columns
      write.
      mode(SaveMode.Append).
      //jdbc(jdbcUrl, "stocks", connectionProperties)
	  jdbc(jdbcUrl, "flights", connectionProperties)
  }
}

// TODO: Read compressed
// option("codec", "org.apache.hadoop.io.compress.GzipCodec").
