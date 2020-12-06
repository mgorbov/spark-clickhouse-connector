package org.apache.spark.clickhouse

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, TRUNCATE}

import java.util
import scala.collection.JavaConverters._

case class ClickhouseTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    schema: StructType)
  extends Table
    with SupportsRead
//    with SupportsWrite
{

  override def capabilities(): util.Set[TableCapability] = {
    Set(BATCH_READ, BATCH_WRITE, TRUNCATE).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    ClickhouseScanBuilder(sparkSession, schema, options)

//  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ???
}
