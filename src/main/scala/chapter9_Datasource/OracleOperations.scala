package chapter9_Datasource

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object OracleOperations {

  def main(args: Array[String]):Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark:SparkSession = SparkSession.builder().master("local")
      .getOrCreate()

    val jdbcOracleDriver  = "oracle.jdbc.driver.OracleDriver"
    val oracleUrl = s"jdbc:oracle:thin:girija/girija@192.168.61.1:1521:XE"
    val oracleTableName = "GIRIJA.emp"



    val oracleDF = spark.read.format("jdbc")
      .option("url",oracleUrl)
      .option("driver",jdbcOracleDriver)
      .option("dbtable",oracleTableName)
      .load()

    //oracleDF.show()
    val filteredODF = oracleDF.where(expr("job = 'MA'"))//.show(false)

    val selectNeededDF = oracleDF.select("ename")

    val distinctManagerDF = oracleDF.select("job").distinct()

    /*oracleDF.explain()

    filteredODF.explain()

    selectNeededDF.explain()

    distinctManagerDF.explain()*/

    /******************************************************************
     * NOTE: As is not needed while ranaming the table
     * (select distinct(job) from GIRIJA.EMP) AS empt (the "AS" should not be used.
     */

    val sqlQuery = """(select distinct(job) from GIRIJA.EMP) empt """

    val pushDownQueryDF = spark.read
      .format("jdbc")
      .option("url",oracleUrl)
      .option("dbtable", sqlQuery)
      .option("driver" , jdbcOracleDriver)
      .load()

    //pushDownQueryDF.explain()
    //pushDownQueryDF.show()

    val csvData = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("src/main/resources/sampleData/empData.txt")
      //.show()


    val props = new Properties()
    props.put("driver","oracle.jdbc.driver.OracleDriver")
    csvData.write.format("jdbc")
      .option("mode","overwrite")//.jdbc(oracleUrl,"empFromCSV",props)/*format("jdbc")
      .option("url" ,oracleUrl)
      .option("driver",jdbcOracleDriver)
      .option("dbtable","empFromCSV")
      .save()



    /*
    Read from sqlite and write it to oracle
     */

    val sqliteProps = new Properties()
    val sqliteUrl = "jdbc:sqlite:src/main/resources/sampleData/flight-data/jdbc/my-sqlite.db"
    sqliteProps.put("driver","org.sqlite.JDBC")

    val sqliteData = spark.read.jdbc(sqliteUrl,"flight_info",sqliteProps)

    /*sqliteData.write.mode("overwrite")
      .jdbc(oracleUrl,"sqliteFlightInfo",props)*/







  }

}
