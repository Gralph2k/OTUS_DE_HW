package ru.otus.spark_hw_17

import org.apache.spark.sql.SparkSession

/**
  * Use this to test the app locally, from sbt:
  * sbt "run ./src/test/scala/ru/otus/spark_hw_17/resources/crime.csv ./src/test/scala/ru/otus/spark_hw_17/resources/offense_codes.csv ./output.csv"
  */
object BostonCrimeLocalApp extends App{
  val (crimeFile, offenceCodesFile, outputFile) = (args(0), args(1), args(2))
  Runner.run(crimeFile, offenceCodesFile, outputFile)
}

object Runner {
  def run(crimeFile: String, offenceCodesFile:String, outputFile: String): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Boston crimes")
      .getOrCreate();
    val crimeDF = spark.read.option("header", "true").option("inferSchema", "true").csv(crimeFile)
    val offenceCodeDF = spark.read.option("header", "true").option("inferSchema", "true").csv(offenceCodesFile)
    val result = BostonCrime.aggregate(spark, crimeDF, offenceCodeDF)
    result.coalesce(1).write.format("com.databricks.spark.csv").save(outputFile)
  }
}
