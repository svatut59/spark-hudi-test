package ru.homecredit.smartdata.spark.hudi

import org.apache.spark.sql.SparkSession

object SparkHudiCreate {
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
        |CREATE TABLE hudi_test_table (
        |  id INT,
        |  name STRING,
        |  age INT,
        |  city STRING
        |) USING hudi
        |PARTITIONED BY (city)
        |OPTIONS (
        |  primaryKey = 'id',
        |  precombineField = 'id'
        |)
        |LOCATION 'file:///tmp/hudi_table'
        |""".stripMargin
      )

    spark.sql(
      """
        |INSERT INTO hudi_test_table VALUES
        |(1, 'John', null, 'New York'),
        |(2, 'Alice', 25, 'Los Angeles'),
        |(3, 'Bob', 45, 'Chicago')
        |""".stripMargin
      )


  }
          }
