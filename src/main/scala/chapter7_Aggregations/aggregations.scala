package chapter7_Aggregations

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object aggregations {

  def main(args: Array[String]):Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder().master("local")
      .getOrCreate()

    val rtlData = spark.read.format("csv").option("header","true").option("inferSchema","true")
      .load("src/main/resources/simple/retail-data/all/*")
    rtlData.show(10)





  }

}
