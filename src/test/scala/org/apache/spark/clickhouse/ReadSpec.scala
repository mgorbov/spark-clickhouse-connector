package org.apache.spark.clickhouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, ShortType, StringType, StructField, StructType}

class ReadSpec extends ITSpec {

  "Connector" should "read data with defined schema" in {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(Array(
      StructField("int8", ShortType, true),
      StructField("int64", LongType, true),
      StructField("uint8", ShortType, true),
      StructField("uint64", LongType, true),
      StructField("float64", DoubleType, true),
      StructField("string", StringType, true),
      StructField("fixed_string", StringType, true),
      StructField("uuid", StringType, true),
      StructField("date", DateType, true),
      StructField("date_time", DateType, true),
      StructField("enum", StringType, true)))

    val df = spark.read.format("org.apache.spark.clickhouse.ClickhouseDataSource")
      .option(ClickhouseOptions.Hosts, "localhost")
//      .option(ClickhouseOptions.User, "")
//      .option(ClickhouseOptions.Db, "")
//      .option(ClickhouseOptions.Password, "")
      .option(ClickhouseOptions.Query, "select * from default.all_types")
      .option(ClickhouseOptions.PartitionColumn, "int8")
      .option(ClickhouseOptions.LowerBound, "1")
      .option(ClickhouseOptions.UpperBound, "2")
      .option(ClickhouseOptions.NumPartitions, "3")
      .schema(schema)
      .load()

    df.show()



  }
}
