package SparkProject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class batsMenData(
                        bName: String,
                        runsScored: Int
                      )

case class flightData(
                       DEST_COUNTRY_NAME: String,
                       ORIGIN_COUNTRY_NAME: String,
                       count: BigInt
                     )

object FlightData1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger(("org")).setLevel(Level.ERROR)

    val ss: SparkSession = SparkSession.builder.master("local")
      .appName("VariousData")
      .getOrCreate()

    ss.conf.set("spark.sql.shuffle.partitions", "5")
    val rtlData = new retailData(ss)
    val rtlDF = rtlData.readData()
    rtlDF.createOrReplaceTempView("retail_data")
    val rtlDataSchema = rtlDF.schema

    //rtlDF.printSchema()

    import ss.implicits._

    /*Read Flight Data from various Formats*/

    /*Parquet Data*/

    val flightDataParquetDF = ss.read
      .parquet("sampleData/flight-data/parquet/2010-summary.parquet/").as[flightData]

    val data = flightDataParquetDF
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(flight_row => flight_row)
      .take(5)

    /*CSV Data */
    val flightDataCSV = ss.read.option("inferSchema", value = true).option("header", value = true)
      .csv("sampleData/FlightData")

    val groupedData = flightDataCSV.groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "frequency")
      .sort(desc("frequency"))
      .limit(5)


    /*retail Data*/

    val maxRetailCustomerPurchase = rtlData.getMaxPurchaseCustomer(rtlDF)
    val retailDataStreamDF = rtlData.rtlDataStream(rtlDF,rtlDataSchema, "sampleData/retail-data/by-day/*.csv")
    ///println(retailDataStreamDF.isStreaming).
    val purchaseBycustomerStream = retailDataStreamDF.selectExpr("CustomerID",
      "InvoiceDate",
      "(Quantity * UnitPrice) as totalCost")
      .groupBy(
        col("CustomerID"), window(col("InvoiceDate"), "1 Day"))
      .sum("totalCost")


  /*  purchaseBycustomerStream.writeStream
      .format("console")
      .queryName("customer_purchase")
      .outputMode("complete")
      .start()

    ss.sql(
      """
        |select *
        |from customer_purchase
        |order by `sum(totalCost)` DESC
        |""".stripMargin).show(5)
*/

    flightDataCSV
      .selectExpr("*","(DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME ) as distenation")
      //.show()

    //flightDataCSV.selectExpr("avg(count) as avg","count(distinct(DEST_COUNTRY_NAME)) as count").show

    //flightDataCSV.withColumn("new Colname",expr("DEST_COUNTRY_NAME")).show(false)


    //println(flightDataCSV.count())
    //flightDataCSV.selectExpr("avg(count)","count(distinct(DEST_COUNTRY_NAME))").show()

    /*flightDataCSV.select(expr("*"),lit(4).as("four"))
      .selectExpr("*","count > four as greatthan4times").show()*/

    //flightDataCSV.withColumn("literal5",lit(5)).show()

    //flightDataCSV.where("count < 2").show()
    //flightDataCSV.sample(withReplacement = true,.1,6).show(100,truncate = false)

    val newSchema = flightDataCSV.schema

    val newData = Seq(
      Row("new Country","Other Country",5),
      Row("new Country2","Other Country3",1)
    )

    val newDataCollection = ss.sparkContext.parallelize(newData)
    val newDF = ss.createDataFrame(newDataCollection,newSchema)

    flightDataCSV
      //.union(newDF)
      //.where("count = 1")
      .where(expr("ORIGIN_COUNTRY_NAME") =!= "United States")
      .orderBy(desc("count"),asc("ORIGIN_COUNTRY_NAME"))
      //.show(false)

    //println(flightDataCSV.rdd.getNumPartitions)

    val x  = (1 to 12).toList
    val df = x.toDF("number")
    //println(df.rdd.partitions.length






  }
}
