package org.apache.spark.clickhouse

import org.apache.spark.sql.connector.read.InputPartition

case class ClickhouseInputPartition(whereClause: String, idx: Int) extends InputPartition
