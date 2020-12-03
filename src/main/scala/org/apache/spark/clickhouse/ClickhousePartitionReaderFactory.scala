package org.apache.spark.clickhouse

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.TaskContext

case class ClickhousePartitionReaderFactory(
    sqlConf: SQLConf,
    schema: StructType,
    clickhouseConfig: ClickhouseConfig)
  extends PartitionReaderFactory with Logging {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[ClickhouseInputPartition])

    val clickhousePartition = partition.asInstanceOf[ClickhouseInputPartition]

    val url = clickhouseConfig.urls(clickhousePartition.idx % clickhouseConfig.urls.size)
    val rs = Utils.executeQuery(url, clickhouseConfig.query)

    val rowsIterator = JdbcUtils.resultSetToSparkInternalRows(rs, schema, TaskContext.get().taskMetrics().inputMetrics)

    ClickhousePartitionReader(rowsIterator)
  }
}
