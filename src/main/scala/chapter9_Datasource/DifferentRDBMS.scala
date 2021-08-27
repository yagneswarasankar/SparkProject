package chapter9_Datasource

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DifferentRDBMS {



  def main(args: Array[String]):Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

    val jdbcDriver  = "org.sqlite.JDBC"
    val path1  = "src/main/resources/simple/flight-data/jdbc/my-sqlite.db"
    val url = s"jdbc:sqlite:${path1}"
    val table_name = "flight_info"

   /* val connection = DriverManager.getConnection(url)
    connection.isClosed
    connection.close()*/

    val tabData = spark.read.format("jdbc").option("url",url)
      .option("dbtable",table_name).option("driver",jdbcDriver).load()

    tabData.show()

    /********************************************************
     * Oracle
     */

   //Localhost to be replaced with ip address and the passworod also has to be changed.

    val jdbcOracleDriver  = "oracle.jdbc.driver.OracleDriver"
    val oracleUrl = s"jdbc:oracle:thin:girija/<Password>@<IPADDRESS>:1521:XE"
    val oracleTableName = "GIRIJA.emp"

    val oracleDt = spark.read.format("jdbc")
      .option("driver",jdbcOracleDriver)
      .option("url",oracleUrl)
      .option("dbtable",oracleTableName)
      //.option("user","girija")
      //.option("password","password")
      .load()

    oracleDt.where(col("JOB") === "MA").explain()

  }
}
