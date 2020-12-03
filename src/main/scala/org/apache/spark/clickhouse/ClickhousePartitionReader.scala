package org.apache.spark.clickhouse

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

case class ClickhousePartitionReader(
    rowIterator: Iterator[InternalRow])
  extends PartitionReader[InternalRow] with Logging {

  override def next(): Boolean = {
    rowIterator.hasNext
  }

  override def get(): InternalRow = {
    rowIterator.next()
  }

  override def close(): Unit = {

  }
}
