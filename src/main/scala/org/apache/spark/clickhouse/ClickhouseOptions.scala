package org.apache.spark.clickhouse

import org.apache.spark.sql.types.DataType

object ClickhouseOptions {
  val Query = "query"
  //  val Url = "url"
  val Cluster = "cluster"
  val Hosts = "host"
  val Port = "port"
  val User = "user"
  val Password = "password"
  val Db = "db"
  val PartitionColumn = "partitionColumn"
  val LowerBound = "lowerBound"
  val UpperBound = "upperBound"
  val NumPartitions = "numPartitions"
  val QueryTimeout = "queryTimeout"

}

case class ClickhouseConfig(
      query: String,
      hosts: List[String],
      port: Int,
      cluster: String,
      db: String,
      user: String,
      password: String,
      timeout: Int) {

  def urls: Seq[String] = hosts.map(host => s"jdbc:clickhouse://$host:${port}")
}

case class ClickhousePartitioningInfo(
      column: String,
      columnType: DataType,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int)