package chapter7_Aggregations

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, grouping_id, sum, to_date}
//import org.apache.spark.sql.functions.{approx_count_distinct, collect_list, collect_set, count, countDistinct, first, last, max, min, sum, sumDistinct}

object aggregations {

  def main(args: Array[String]):Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder().master("local")
      .getOrCreate()

    val rtlData = spark.read.format("csv").option("header","true").option("inferSchema","true")
      .load("src/main/resources/simple/retail-data/all/*")
/*
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




    /***********************************************************************************
    *                                       Grouping
    ************************************************************************************/

    rtlData.groupBy("InvoiceNo","CustomerID").count().show(false)
    rtlData.groupBy("InvoiceNo").agg(
      count("quantity").as("quantity"),
       expr("count(quantity)"))
      .show(4,false)



    //2010-12-01 08:26:00

    val forWindoFuncSpec = rtlData.withColumn("date",to_date(col("InvoiceDate"),"MM/d/yyyy H:mm"))
    val windowSpec = Window
      .partitionBy("CustomerID","date")
      .orderBy(col("quantity").desc)
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)


    val maxpurchaseCustomer = max(col("quantity")).over(windowSpec)
    val denseRank = dense_rank().over(windowSpec)
    val rank1  = rank().over(windowSpec)

    forWindoFuncSpec.select(col("customerID"),col("date"),col("quantity"),col("Description"),
      denseRank.as("denseRank"),
      rank1.as("rank1"),
      maxpurchaseCustomer.alias("maxpurchase")).show(false)

 */
    /***********************************************************************
     *                             Rollup and Cube and group_id
     ***********************************************************************/


    val rtlDatanotNull = rtlData.withColumn("date",to_date(col("InvoiceDate"),"MM/d/yyyy H:mm")).drop()

    rtlDatanotNull.rollup("date","country").agg(sum("quantity").as("sumQty"))
      .select(col("date"),col("country"),col("sumqty"))
      .orderBy("date")
      //.show()

    rtlDatanotNull.cube("date","country").agg(sum("quantity").as("sumQty"))
      .select(col("date"),col("country"),col("sumqty"))
      .orderBy("date")
      //.show(100,truncate = false)

    rtlDatanotNull.cube("date","country").agg(grouping_id().as("grpid"),sum("quantity").as("sumQty"))
      .select(col("date"),col("country"),col("grpid"),col("sumqty"))
      .orderBy("date")
      //.show(100,truncate = false)

    /**********************************************************************************
     *                                  pivot
     ******************************************************************************/

    val rtlDatanotpivot = rtlData.withColumn("date",to_date(col("InvoiceDate"),"MM/d/yyyy H:mm")).drop()

    rtlDatanotpivot.where(col("country") === "USA").groupBy("date").pivot("country").sum()
      //.show()

    import spark.implicits._
    val studentDF  = Seq(
      ("Girija","Maths",100),
      ("Girija","Science",93),
      ("Hari","Maths",100),
      ("Hari","Science",94)).toDF("name","subject","marks")

    studentDF.groupBy("name").pivot("subject").sum("marks")
     // .show()

    studentDF.groupBy("name").pivot("subject").avg("marks")
      .show()

    /*************************
     * output - Rows turned into Columns
     * +------+-----+-------+
     * |  name|Maths|Science|
     * +------+-----+-------+
     * |  Hari|100.0|   94.0|
     * |Girija|100.0|   93.0|
     * +------+-----+-------+
     */



  }


}
