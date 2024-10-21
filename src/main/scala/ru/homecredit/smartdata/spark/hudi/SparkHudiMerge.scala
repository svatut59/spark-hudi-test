package ru.homecredit.smartdata.spark.hudi

import org.apache.spark.sql.SparkSession

object SparkHudiMerge {
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
      """
        |CREATE OR REPLACE TEMP VIEW new_data AS
        |SELECT * FROM (
        |  SELECT 1 AS id, null AS name, 31 AS age, 'New York' AS city
        |  UNION ALL
        |  SELECT 4 AS id, 'Diana' AS name, 28 AS age, 'San Francisco' AS city
        |  UNION ALL
        |  SELECT 2 AS id, 'Alice Updated' AS name, 26 AS age, 'Los Angeles' AS city
        |  UNION ALL
        |  SELECT 5 AS id, 'Eve' AS name, 22 AS age, 'Seattle' AS city
        |)
        |""".stripMargin
    )

    spark.sql(
      """
        |MERGE INTO hudi_test_table AS t
        |USING new_data AS s
        |ON t.id = s.id
        |WHEN MATCHED THEN
        |  UPDATE SET t.name = s.name, t.age = s.age, t.city = s.city
        |WHEN NOT MATCHED THEN
        |  INSERT (id, name, age, city) VALUES (s.id, s.name, s.age, s.city)
        |""".stripMargin
    )
  }
}
