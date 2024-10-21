package ru.homecredit.smartdata.spark.hudi

import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.SparkSession

object SparkHudiUpsert {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Spark Hudi Test App")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val tablePath = "file:///tmp/hudi_table"

    val newData = Seq(
      (1, null, 31, "New York"),
      (4, "Diana", 28, "San Francisco"),
      (2, "Alice Updated", 26, "Los Angeles"),
      (5, "Eve", 22, "Seattle")
    ).toDF("id", "name", "age", "city")

    newData.write
      .format("hudi")
      .option(HoodieWriteConfig.TBL_NAME.key, "hudi_test_table")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.datasource.write.precombine.field", "id")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.payload.class", "org.apache.hudi.common.model.PartialUpdateAvroPayload")
      .mode(SaveMode.Append)
      .save(tablePath)
  }
}
