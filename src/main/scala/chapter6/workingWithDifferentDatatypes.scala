package chapter6


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, date_add, date_sub, datediff, expr, floor, initcap, lit, lower, lpad, ltrim, monotonically_increasing_id, months_between, regexp_extract, regexp_replace, round, rpad, rtrim, to_date, translate, trim}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object workingWithDifferentDatatypes {


def main(args: Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local")
    .getOrCreate()

  val rtlData = spark.read.option("header","true")
    .option("inferSchema","true")
    .csv("src/main/resources/simple/retail-data/by-day/2010-12-01.csv")

  rtlData.printSchema()

  rtlData.show()

  /**************************************************
   * Literals
   *************************************************/
  rtlData.select(expr("*"),lit(5),lit("five"),lit(5.0)).show()

  /****************************************************
   * Boolean
   ****************************************************/
  rtlData.where(col("InvoiceNo").equalTo(536365))
    .select("InvoiceNo","Description").show(5)

  rtlData.where(col("InvoiceNo") ===536365)
    .select("InvoiceNo","Description").show(5)

  rtlData.where("InvoiceNo = 536365").show(5)

  val priceFilter = col("UnitPrice" ) > 600
  val descriptionFilter = col("Description").contains("POSTAGE")

  rtlData.where(col("stockCode") isin("DOT")).where(priceFilter.or(descriptionFilter))
    .show()

  /*******************************************************************
   *                          Numbers
   *****************************************************************/
  //Rounding - round function
  rtlData.select(round(col("unitprice"),1).alias("rounded"),col("unitprice"))
    .show(5)

  rtlData.select(round(lit(2.5)),floor(lit(2.6))).show(1)

  rtlData.describe().show()

  rtlData.select(monotonically_increasing_id()).show(2)

  /*************************************************************
   * Strings
   ************************************************************/

  rtlData.select(initcap(col("Description"))).show(5)

  rtlData.select(col("Description"),lower(col("Description")),upper(col("Description")))
    .show()

  rtlData.select(
    ltrim(lit("     HELLO  ")).as("ltrim"),
    rtrim(lit("     HELLO  ")).as("rtrim"),
    trim(lit("       HELLO  ")).as("trim"),
    rpad(lit("Hello"),16," ").as("rpad"),
    lpad(lit("Hello"),16," ").as("lpad"),
    rpad(lit("This is a test"),6," "),
    lpad(lit("This is a test"),6," ")
  ).show(5,false)

  val colorList = List("BLACK","WHITE","GREEN","RED","GREEN","BLUE")
  val regExp = colorList.map(_.toUpperCase).mkString("|")

  rtlData.select(regexp_replace(col("Description"),regExp,"COLOR")).as("regExpReplace")
    .show(5,false)

  val tranlateCond = translate(col("Description"),"LEFT", "1337")

  rtlData.select(tranlateCond,col("Description")).show(5,false)

  val regExpStr = colorList.map(_.toUpperCase).mkString("(","|",")")
  rtlData.select(regexp_extract(col("Description"),regExpStr,1).as("firstColor"),col("description"))
    .show(5,truncate = false)

  val containBlack = col("Description").contains("BLACK")
  val containWhite = col("Description").contains("WHITE")

  rtlData.withColumn("containsBlackOrWhile",containBlack.or(containWhite))
    .where("containsBlackOrWhile").show()


  val selectColumns = colorList.map(color =>{
    col("Description").contains(color).alias(s"is_$color")
  }):+ expr("*")

  println(selectColumns.mkString(","))

  rtlData.select(selectColumns :_*)
  .where(col("is_RED").or(col("is_WHITE"))).show(5)

  /**************************************************************
   *                           TimeStamps
   **************************************************************/

  val dateDf = spark.range(3).as("Number")
    .withColumn("today",current_date())
    .withColumn("currntTime",current_timestamp())


  dateDf.select(expr("*"),date_add(col("today"),5),
    date_sub(col("today"),7).as("weekago"))
    .select(expr("*"),datediff(col("today"),col("weekago")))
    .show(false)

  dateDf.select(expr("*"),
    to_date(lit("2020-08-18")).as("start"))
    .select(round(months_between(col("today"),col("start")).as("months"))
  ).show()

  val dateformat  = "yyyy.MMMMM.dd"
  spark.range(1)
    .select(to_date(current_date,"yyyy.MMMMM.dd"),
      to_date(lit("2021.JUNE.21"),"yyyy.MMMMM.dd"))
    .show()

  val schema = StructType(Array(
    StructField("id",IntegerType,nullable = true),
    StructField("name",StringType,nullable=true)))

  val data = Seq(Row(null,"Girija"),
    Row(null,null))
   val rdd = spark.sparkContext.parallelize(data)
   val df = spark.createDataFrame(rdd,schema)
  df.show()
  df.na.drop().show()
  df.na.fill("Girija":String).show()
  val fillColValues = Map("id" -> 4,"name" -> "Thisis a null value")
  df.na.fill(fillColValues).show()




















}

}
