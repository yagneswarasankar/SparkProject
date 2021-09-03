package chapter10_SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, sum}


object SparkSql {



  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]):Unit ={
    val logger = Logger.getLogger(this.getClass.getName)

    val spark: SparkSession = SparkSession.builder().master("local")
      .getOrCreate()
    logger.info("The spark session created")

    val jsonDF = spark.read.format("json")
      .load("src/main/resources/sampleData/flight-data/json/2015-summary.json")
      //.createTempView("flight_info")


    jsonDF.createTempView("flight_info")//select * from flight_info").show()

    spark.sql("select DEST_COUNTRY_NAME,sum(count) as countsum from flight_info" +
      " group by DEST_COUNTRY_NAME")
      .where("DEST_COUNTRY_NAME like 'S%'")
      .where("countsum > 10")
      .show(truncate = false)

    jsonDF.groupBy("DEST_COUNTRY_NAME")
      .agg(sum("count").as("countsum"))
      .where("DEST_COUNTRY_NAME like 'S%'")
      .where("countsum > 10")
      .show(truncate = false)

    jsonDF.select(
      col("DEST_COUNTRY_NAME"),
      expr("ORIGIN_COUNTRY_NAME")
      //,'ORIGIN_COUNTRY_NAME
     // ,$"count"
    ).show()
  }
}
