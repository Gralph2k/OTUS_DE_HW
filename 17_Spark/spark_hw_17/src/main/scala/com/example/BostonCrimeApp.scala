package com.example

import org.apache.spark.sql.SparkSession

/*
   Use this to test the app locally, from sbt:
   sbt "run ./src/test/scala/com/example/resources/crime.csv ./src/test/scala/com/example/resources/offense_codes.csv ./output"

   or

   spark-submit --master local[*] --class com.example.BostonCrimeMap ./target/scala-2.11/spark_hw_17-assembly-0.0.1.jar \
   ~/Documents/OTUS/DataEnginer/projects/17_Spark/crimes-in-boston/crime.csv \
   ~/Documents/OTUS/DataEnginer/projects/17_Spark/crimes-in-boston/offense_codes.csv \
   ~/Documents/OTUS/DataEnginer/projects/17_Spark/crimes-in-boston/output
*/
object BostonCrimeMap extends App{
  val (crimeFile, offenceCodesFile, outputFile) = (args(0), args(1), args(2))
  Runner.run(crimeFile, offenceCodesFile, outputFile)
}


object Runner {
  def run(crimeFile: String, offenceCodesFile:String, outputFile: String): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("BostonCrimeMap")
      .getOrCreate();
    val crimeDF = spark.read.option("header", "true").option("inferSchema", "true").csv(crimeFile)
    val offenceCodeDF = spark.read.option("header", "true").option("inferSchema", "true").csv(offenceCodesFile)
    val result = BostonCrime.aggregate(spark, crimeDF, offenceCodeDF)
    result.coalesce(1).write.mode("overwrite").parquet(outputFile)
  }
}
