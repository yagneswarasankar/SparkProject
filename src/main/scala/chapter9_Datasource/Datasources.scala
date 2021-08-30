package chapter9_Datasource

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,split}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Datasources {

  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

    /**********************************************************************************
     *                                          CSV format
     **********************************************************************************/

    val schema1 =   StructType{
      Array(
          StructField("ORIGIN_COUNTRY_NAME",StringType, nullable = true),
          StructField("DEST_COUNTRY_NAME", StringType,nullable = true),
          StructField("count", IntegerType,nullable = true)
      )
    }

   val schema =   new StructType()
     .add("DEST_COUNTRY_NAME",StringType, nullable = true)
     .add("ORIGIN_COUNTRY_NAME", StringType,nullable = true)
     .add("count", IntegerType,nullable = true)
     .add("_currupt_record",StringType,nullable = true)

    val flightData = spark.read.format("csv")
      .option("header","true")
      .option("columnNameOfCorruptRecord","_currupt_record")
      .option("mode","PERMISSIVE")
      .schema(schema)
      .load("sampleData/flight-data/csv/2010-summary.csv")

      flightData.show(5,truncate = false)

    /* As the integer data is written as String so it is deceptive records.
    +-----------------+-------------------+-----+-----------------------------+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|_currupt_record              |
    +-----------------+-------------------+-----+-----------------------------+
    |United States    |Romania            |1    |null                         |
    |null             |null               |null |United States,Canada,Srinivas|
    |United States    |India              |69   |null                         |
    |Egypt            |United States      |24   |null                         |
    |Equatorial Guinea|United States      |1    |null                         |
    +-----------------+-------------------+-----+-----------------------------+
     */

    val flightData1 = spark.read.format("csv")
      .option("header","true")
      .option("columnNameOfCorruptRecord","_currupt_record")
      .option("mode","dropMalformed")
      .schema(schema)
      .load("sampleData/flight-data/csv/2010-summary.csv")

    // As the USA record is malformed (with String data in the integer field) it is dropped
    // United States,Canada,Srinivas

    flightData1.show(5,false)

    /*
    +-----------------+-------------------+-----+---------------+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|_currupt_record|
    +-----------------+-------------------+-----+---------------+
    |United States    |Romania            |1    |null           |
    |United States    |India              |69   |null           |
    |Egypt            |United States      |24   |null           |
    |Equatorial Guinea|United States      |1    |null           |
    |United States    |Singapore          |25   |null           |
    +-----------------+-------------------+-----+---------------+
     */


    /***************************************************************************
     * Writing the data to CSV
     */

    val flightData2 = spark.read.format("csv")
      .option("header","true")
      .option("columnNameOfCorruptRecord","_currupt_record")
      .option("mode","dropMalformed")
      .load("sampleData/flight-data/csv/2010-summary.csv")


    flightData2.write.format("csv").option("sep","\t").mode("overwrite")
      .save("src/main/resources/cscwriting/flight.csv")


    /*******************************************************************************
     * JSON
     *
     */
    val jsonFile = spark.read.format("json").option("mode","FAILFAST")
      .option("inferSchema","true")
      .load("sampleData/flight-data/json/2010-summary.json")

     jsonFile.show()

    jsonFile.write.format("json").mode("overwrite")
    .save("src/main/resources/jsonwriting/flightdata.json")


    /**********************************************************************
     * Parquet File (Default file format for Spark
     */

    val parquetFile = spark.read.load("sampleData/flight-data/parquet/2010-summary.parquet")

    parquetFile.show(5,truncate = false)

    flightData.write.format("parquet")
      .mode("overwrite")
      .save("src/main/resources/parquetwriting/flightdata/parquet")

    spark.read.format("parquet")
      .load("src/main/resources/parquetwriting/flightdata/parquet/*")
      .show(false)

    /**********************************************************************
     * ORC File oprimized to read with Hive
     */

    val orcFile = spark.read.format("orc").load("sampleData/flight-data/orc/2010-summary.orc")

    orcFile.show(5,false)

    flightData.write.format("orc").mode("overwrite")
      .save("src/main/resources/orcwriting/flightdata/orc/")




    /*********************************************************************
     *                             Text Files
     ********************************************************************/

    val textFileDF = spark.read.textFile("src/main/resources/sampleData/flight-data/csv/2010-summary.csv")

    textFileDF.select(split(col("value"),",").as("rows")).show(false)

    val flightDatacsv = spark.read.format("csv")
      .option("header","true")
      .option("columnNameOfCorruptRecord","_currupt_record")
      .option("mode","PERMISSIVE")
      .load("src/main/resources/sampleData/flight-data/csv/2010-summary.csv")

    flightDatacsv.select(col("DEST_COUNTRY_NAME"))
      .write.mode("overwrite")
      .text("src/main/resources/textfile/flightdata/text/")


    flightDatacsv.limit(10).write.mode("overwrite")
      .partitionBy("DEST_COUNTRY_NAME")
      .save("src/main/resources/parquet/partitioned-files.parquet")

    flightDatacsv.limit(10).selectExpr("DEST_COUNTRY_NAME","count")
      .write.mode("overwrite").partitionBy("count")
      .text("src/main/resources/textfile/flightdata/partition/text/")

    flightDatacsv.repartition(5).write.mode("overwrite")
      .csv("src/main/resources/csv/multiplefiles/")


    /********************************************************
     * This is not writingn the files in the way expected. Thought
     * this will sagregate the data as per country But although the partitions are getting
     * created as expected the data in them is not the way expected.
     */
    flightDatacsv.write
      .partitionBy("DEST_COUNTRY_NAME")
      .mode("overwrite")
      .format("csv")
      .save("src/main/resources/csv/partitioned-csv-files")

  }

}
