package chapter7_Aggregations

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, collect_list, collect_set, count, countDistinct, first, last, max, min, sum, sumDistinct}

object aggregations {

  def main(args: Array[String]):Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder().master("local")
      .getOrCreate()

    val rtlData = spark.read.format("csv").option("header","true").option("inferSchema","true")
      .load("src/main/resources/simple/retail-data/all/*")

    /************************************************************************************
     *                                         Count
     ************************************************************************************/

    rtlData.select(count("stockcode").alias("count"))
      .show(truncate = false)

    rtlData.select(countDistinct("stockcode").alias("contdistinct"))
      .show(truncate = false)

    rtlData.select(approx_count_distinct("stockcode",0.1).as("approximatecount")).show()

    rtlData.select(first("stockcode"),last("stockcode"))
      .show(truncate = false)

    rtlData.select(min("quantity"),max("quantity")).show()


    rtlData.select(sum("quantity").as("sum"),
                   sumDistinct("quantity").as("sumDisinct"))
                  .show()



    /***********************************************************************************
     * Aggregating complex data types
     ***********************************************************************************/

    rtlData.agg(collect_set("country"),collect_list("country"))
      .show()













  }

}
