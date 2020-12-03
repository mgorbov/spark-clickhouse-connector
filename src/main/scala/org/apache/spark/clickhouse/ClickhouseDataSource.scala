package org.apache.spark.clickhouse

import java.util

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

class ClickhouseDataSource extends DataSourceRegister with TableProvider {

  private lazy val sparkSession = SparkSession.active


  override def shortName(): String = "clickhouse"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = ???

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]
                       ): Table = {
    assert(partitioning.isEmpty)

    require(properties.containsKey(ClickhouseOptions.Hosts),
      s"Option ${ClickhouseOptions.Hosts} is required")
    require(properties.containsKey(ClickhouseOptions.Query),
      s"Option ${ClickhouseOptions.Query} is required")

    ClickhouseTable(shortName(), sparkSession, new CaseInsensitiveStringMap(properties), schema)
  }
}



