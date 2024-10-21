package ru.homecredit.smartdata.spark.hudi

import org.apache.spark.sql.SparkSession

object SparkHudiShow {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Spark Hudi Test App")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .master("local[*]")
      .getOrCreate()

    spark.sql(
      """
        |CREATE TABLE hudi_test_table
        |USING hudi
        |LOCATION 'file:///tmp/hudi_table'
        |""".stripMargin
    )

    spark.sql(
      """SELECT * FROM hudi_test_table""".stripMargin
    ).show()
  }
}
