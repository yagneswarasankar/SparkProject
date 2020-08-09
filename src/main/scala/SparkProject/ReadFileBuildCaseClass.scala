package SparkProject

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.IntegerType


object ReadFileBuildCaseClass{

  def main(args: Array[String]) : Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss: SparkSession = org.apache.spark.sql.SparkSession.builder.master ("local")
    .appName ("SimpleScalaProject")
    .getOrCreate()


    val odiPlayerInnings20thCentury = ss.read.format ("csv").option ("header", "true")
     .csv("src/main/resources/Men ODI Player Innings Stats - 20th Century.csv")

    val odiPlayerInnings21thCentury = ss.read.format ("csv").option ("header", "true")
      .csv("src/main/resources/Men ODI Player Innings Stats - 21st Century.csv")

    val odiPlayerInnings = odiPlayerInnings20thCentury union odiPlayerInnings21thCentury

    val odiPlayerInningsDF  = odiPlayerInnings//.filter(row => row.getAs[String]("Innings Player") == "M Hayward")
    .withColumn("Runs", when(col("Innings Runs Scored Num").cast(IntegerType).isNull,0).otherwise(col("Innings Runs Scored Num"))
      .cast(IntegerType))
     .filter(row => row.getAs[Int]("Runs") != 0)

    odiPlayerInningsDF.show(100,false)

    //odiPlayerInningsDF.select("country","Innings Player","Runs").show(100)
    val groupedDataset = odiPlayerInningsDF.
        groupBy("country","Innings Player")//.max("Runs").as("maximum Rns")

  }

}
