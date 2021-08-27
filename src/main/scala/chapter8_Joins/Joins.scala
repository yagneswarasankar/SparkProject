package chapter8_Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins {

  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._
    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100)))
      .toDF("id", "name", "graduate_program", "spark_status")
    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")
    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")

    /***************************************************
     *                   Inner Join
     ***************************************************/

    var joinType = "inner"
    val joinDFCond = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram,joinDFCond,joinType)
      .show(false)

    /******************************************************************
     *                             Outer Joins
     ******************************************************************/

    joinType = "outer"
    person.join(graduateProgram,joinDFCond,joinType)
      .show(false)

    joinType = "left_outer"
    graduateProgram.join(person,joinDFCond,joinType)
      .show(false)

    joinType = "right_outer"
    person.join(graduateProgram,joinDFCond,joinType)
      .show(false)

    /**********************************************************
     *                    Semi Join
     *********************************************************/

    joinType = "left_semi"
    graduateProgram.join(person,joinDFCond,joinType)
      //.show()

    val gradProgram2 = graduateProgram.union(Seq(
      (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
    gradProgram2.join(person,joinDFCond,joinType)
      //.show()

    /*****************************************************************
     * left_anti
     ****************************************************************/
    joinType = "left_anti"
    graduateProgram.join(person,joinDFCond,joinType)
      .show()

    /**************************************************************
     * Cross Join
     **************************************************************/
    person.crossJoin(graduateProgram)
      .show()



    /******************************************************************
     * Join with complex type
     */

    person.withColumnRenamed("id","personid")
      .join(sparkStatus,expr(" array_contains(spark_Status,id)"))
      .show()

    person.withColumnRenamed("id","personid")
      .join(sparkStatus,expr("not array_contains(spark_Status,id)"))
      .show()


  }
}
