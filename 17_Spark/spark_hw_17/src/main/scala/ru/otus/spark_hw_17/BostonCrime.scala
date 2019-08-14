package ru.otus.spark_hw_17


import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object BostonCrime {
  def aggregate(sc: SparkSession, crimeDF: DataFrame, offenceCodeDF: DataFrame): DataFrame = {


    val cachedCrime =
      crimeDF
      .withColumn("DISTRICT", coalesce(col("DISTRICT"),lit("n/a")))
      .cache
    cachedCrime.createOrReplaceTempView("crime")

    val offenceCode =
      offenceCodeDF
      .withColumn("CRIME_TYPES", split(col("Name"), " - "))
      .withColumn("CRIME_TYPE", when(size(col("CRIME_TYPES")).geq(lit(1)), col("CRIME_TYPES").getItem(0)).otherwise(lit("n/a")))

    val total =
      cachedCrime
        .groupBy("DISTRICT")
        .agg(
          count(col("*")).as("crimes_total")
          , avg(col("lat")).as("lat")
          , avg(col("long")).as("lng")
        )

    val median =
      sc.sql(
        """
                SELECT a.DISTRICT
                      ,percentile_approx(crimes, 0.5) crimes_monthly
                  FROM (
                        SELECT a.DISTRICT
                              ,a.YEAR*100+a.MONTH YearMonth
                              ,COUNT(*) crimes
                          FROM crime a
                          GROUP BY a.DISTRICT,a.YEAR*100+a.MONTH
                       ) a
                  GROUP BY a.DISTRICT
                """)

    val crime_type =
      cachedCrime.as("a")
        .join(offenceCode.as("b"), col("a.OFFENSE_CODE") === col("b.CODE") && col("a.OFFENSE_DESCRIPTION") === col("b.NAME"))
        .groupBy("DISTRICT", "CRIME_TYPE")
        .agg(count(lit(1)).as("CRIMES"))
        .withColumn("RN", row_number().over(Window.partitionBy("DISTRICT").orderBy(col("CRIMES").desc)))
        .filter("RN<=3")
        .withColumn("top1", when(col("RN") === 1, col("CRIME_TYPE")))
        .withColumn("top2", when(col("RN") === 2, col("CRIME_TYPE")))
        .withColumn("top3", when(col("RN") === 3, col("CRIME_TYPE")))
        .groupBy("DISTRICT")
        .agg(max(col("top1")).as("top1")
          , max(col("top2")).as("top2")
          , max(col("top3")).as("top3")
        )
        .withColumn("frequent_crime_types", concat_ws(", ", col("top1"), col("top2"), col("top3")))

    total
      .join(median, Seq("DISTRICT"), "LEFT")
      .join(crime_type, Seq("DISTRICT"), "LEFT")
      .select("DISTRICT","crimes_total","crimes_monthly", "frequent_crime_types", "lat", "lng")
      .na.fill(0)
  }
}
