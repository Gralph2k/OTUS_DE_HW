package ru.otus.spark_HW_17

/**
  * A simple test
  */

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import ru.otus.spark_hw_17.BostonCrime


class BostonCrimeTest extends FunSuite with SharedSparkContext {
  test("simple test") {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Test Boston crimes")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val crimeDF = spark.read.option("header", "true").option("inferSchema", "true").csv("./src/test/scala/ru/otus/spark_hw_17/resources/crime.csv")
    val offenceCodeDF = spark.read.option("header", "true").option("inferSchema", "true").csv("./src/test/scala/ru/otus/spark_hw_17/resources/offense_codes.csv")
    val expected = spark.read.option("header", "true").option("inferSchema", "true").csv("./src/test/scala/ru/otus/spark_hw_17/resources/expected.csv")

    val result = BostonCrime.aggregate(spark, crimeDF, offenceCodeDF).cache()

    //    result.repartition(1).write.mode("overwrite").format("csv").option("header","true").csv("./resources/expected")
    assert(result.except(expected).count().equals(0L))
    assert(expected.except(result).count().equals(0L))

    result.show()
    result.unpersist()
  }
}


