package com.example

/**
  * A simple test
  */

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite


class BostonCrimeTest extends FunSuite with SharedSparkContext {
  test("simple test") {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Test Boston crimes")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val resourcePath = "./src/test/scala/com/example/resources"
    val crimeDF = spark.read.option("header", "true").option("inferSchema", "true").csv(resourcePath + "/crime.csv")
    val offenceCodeDF = spark.read.option("header", "true").option("inferSchema", "true").csv(resourcePath + "/offense_codes.csv")
    val expected = spark.read.option("header", "true").option("inferSchema", "true").csv(resourcePath + "/expected.csv")

    val result = BostonCrime.aggregate(spark, crimeDF, offenceCodeDF).cache()

    assert(result.except(expected).count().equals(0L))
    assert(expected.except(result).count().equals(0L))

    result.show()
    result.unpersist()
  }
}




