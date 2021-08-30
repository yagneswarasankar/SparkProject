package chapter9_Datasource

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

object DifferentRDBMS {



  def main(args: Array[String]):Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()


    /**************************************************************************
     * sqlite
     **************************************************************************/
    val jdbcDriver  = "org.sqlite.JDBC"
    val path1  = "sampleData/flight-data/jdbc/my-sqlite.db"
    val url = s"jdbc:sqlite:${path1}"
    val table_name = "flight_info"

   /* val connection = DriverManager.getConnection(url)
    connection.isClosed
    connection.close()*/

    val tabData = spark.read.format("jdbc").option("url",url)
      .option("dbtable",table_name).option("driver",jdbcDriver).load()

    //tabData.show()

    /********************************************************
     * Oracle
     */

   //Localhost to be replaced with ip address and the passworod also has to be changed.

    val jdbcOracleDriver  = "oracle.jdbc.driver.OracleDriver"
    val oracleUrl = s"jdbc:oracle:thin:girija/girija@192.168.61.1:1521:XE"
    val oracleTableName = "GIRIJA.emp"

    val oracleDt = spark.read.format("jdbc")
      .option("driver",jdbcOracleDriver)
      .option("url",oracleUrl)
      .option("dbtable",oracleTableName)
      .load()

  /*  //oracleDt.where(col("JOB") === "MA").explain()

    /**************************************************************************
     *                                       postgres
     **************************************************************************/

    val jdbcpostgresDriver = "org.postgresql.Driver"
    val postgresconnectionString = "jdbc:postgresql:testdb"//192.168.61.1"
    val postgresTableName = "company"

    val postgresDF = spark.read.format("jdbc")
      .option("url",postgresconnectionString)
      .option("driver",jdbcpostgresDriver)
      .option("user","postgres")
      .option("password","girija")
      .option("dbtable",postgresTableName)
      .load()

     /*val connection = DriverManager.getConnection(url)
        connection.isClosed
        connection.close()*/
     //postgresDF.show(false)

    /*
    +---+-----+---+--------------------------------------------------+-------+----------+
    |id |name |age|address                                           |salary |join_date |
    +---+-----+---+--------------------------------------------------+-------+----------+
    |1  |Paul |32 |California                                        |20000.0|2001-07-13|
    |2  |Allen|25 |Texas                                             |null   |2007-12-13|
    |3  |Teddy|23 |Norway                                            |20000.0|null      |
    +---+-----+---+--------------------------------------------------+-------+----------+
     */

    /**********************************************************************
     *                                Mysql
     **********************************************************************/

    val mysqlDriver = "com.mysql.jdbc.Driver"
    val mysqlUrl = "jdbc:mysql://192.168.61.1:3306/sakila"
    val tblname = "country"

    val mysqlDF = spark.read.format("jdbc")
      .option("url",mysqlUrl)
      .option("driver",mysqlDriver)
      .option("dbtable",tblname)
      .option("user","root")
      .option("password","girija")
      .load()
    mysqlDF.show()
*/

    /**********************************************************
     * Sqlite is used for this.
     */

    val props = new Properties()
    val sqliteUrl = "jdbc:sqlite:src/main/resources/sampleData/flight-data/jdbc/my-sqlite.db"
    props.put("driver","org.sqlite.JDBC")

    val predicateArray = Array(
      "DEST_COUNTRY_NAME = 'Sweden' or ORIGIN_COUNTRY_NAME = 'Sweden'",
      "DEST_COUNTRY_NAME = 'Anguilla' or ORIGIN_COUNTRY_NAME = 'Anguilla'"
    )

    val sqliteTable = "flight_info"

    println(spark.read.jdbc(sqliteUrl,sqliteTable,predicateArray,props)
      .rdd.getNumPartitions) //----------------2

    val sqliteFlightData  = spark.read.jdbc(sqliteUrl,sqliteTable,predicateArray,props)

    sqliteFlightData.agg(max(col("count"))).show()


  }
}
