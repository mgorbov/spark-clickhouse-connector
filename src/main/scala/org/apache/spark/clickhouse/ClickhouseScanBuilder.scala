package org.apache.spark.clickhouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ClickhouseScanBuilder(
    sparkSession: SparkSession,
    schema: StructType,
    options: CaseInsensitiveStringMap)
  extends ScanBuilder
//    with SupportsPushDownRequiredColumns
//    with SupportsPushDownFilters
{

  override def build(): Scan =
    ClickhouseScan(sparkSession, schema, options)

//  override def pruneColumns(requiredSchema: StructType): Unit = ???
//
//  override def pushFilters(filters: Array[Filter]): Array[Filter] = ???
//
//  override def pushedFilters(): Array[Filter] = ???
}
