package org.apache.spark.clickhouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ClickhouseScan(
    sparkSession: SparkSession,
    readSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends Scan
    with Batch {

  private val clickhouseConfig = ClickhouseConfig(
    options.get(ClickhouseOptions.Query),
    options.get(ClickhouseOptions.Hosts).split(",").toList,
    options.getOrDefault(ClickhouseOptions.Port, "8123").toInt,
    options.get(ClickhouseOptions.Cluster),
    options.get(ClickhouseOptions.Db),
    options.get(ClickhouseOptions.User),
    options.get(ClickhouseOptions.Password),
    options.getInt(ClickhouseOptions.QueryTimeout, 30 * 1000)
  )

  override def planInputPartitions(): Array[InputPartition] = {
    Utils.columnPartition(readSchema, "", options)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    ClickhousePartitionReaderFactory(sparkSession.sessionState.conf, readSchema, clickhouseConfig)
  }

  override def toBatch: Batch = this
//  override def estimateStatistics(): Statistics = ???
}
