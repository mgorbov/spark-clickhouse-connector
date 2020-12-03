package org.apache.spark.clickhouse

import java.sql.ResultSet
import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, stringToDate, stringToTimestamp}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.clickhouse.settings.{ClickHouseProperties, ClickHouseQueryParam}
import ru.yandex.clickhouse.{ClickHouseDataSource => JdbcDataSource}
import scala.collection.mutable.ArrayBuffer


object Utils extends Logging {

  def executeQuery(url: String, query: String, clickhouseConfig: ClickhouseConfig): ResultSet = {
    val props = new Properties()
    props.setProperty(ClickHouseQueryParam.DATABASE.getKey, clickhouseConfig.db)
    props.setProperty(ClickHouseQueryParam.USER.getKey, clickhouseConfig.user)
    props.setProperty(ClickHouseQueryParam.PASSWORD.getKey, clickhouseConfig.password)

    val clickHouseProps = new ClickHouseProperties()
    logInfo(s"JDBC Connection string: \n$url")
    val dataSource = new JdbcDataSource(url, clickHouseProps)
    val conn = dataSource.getConnection

    conn.createStatement().executeQuery(query)
  }

  def columnPartition(
                       schema: StructType,
                       timeZoneId: String,
                       options: CaseInsensitiveStringMap): Array[InputPartition] = {

    val partitioning = {
      import JDBCOptions._

      val partitionColumn = options.get(ClickhouseOptions.PartitionColumn)
      val lowerBound = options.get(ClickhouseOptions.LowerBound)
      val upperBound = options.get(ClickhouseOptions.UpperBound)
      val numPartitions = options.getInt(ClickhouseOptions.NumPartitions, 0)

      if (partitionColumn.isEmpty) {
        assert(lowerBound.isEmpty && upperBound.isEmpty, "When 'partitionColumn' is not " +
          s"specified, '${ClickhouseOptions.LowerBound}' and '${ClickhouseOptions.UpperBound}' are expected to be empty")
        null
      } else {
        assert(lowerBound.nonEmpty && upperBound.nonEmpty && numPartitions > 0,
          s"When 'partitionColumn' is specified, '$JDBC_LOWER_BOUND', '$JDBC_UPPER_BOUND', and " +
            s"'$JDBC_NUM_PARTITIONS' are also required")

        val (column, columnType) = verifyAndGetNormalizedPartitionColumn(schema, partitionColumn)

        val lowerBoundValue = toInternalBoundValue(lowerBound, columnType, timeZoneId)
        val upperBoundValue = toInternalBoundValue(upperBound, columnType, timeZoneId)

        ClickhousePartitioningInfo(column, columnType, lowerBoundValue, upperBoundValue, numPartitions)
      }
    }

    if (partitioning == null || partitioning.numPartitions <= 1 ||
      partitioning.lowerBound == partitioning.upperBound) {
      return Array[InputPartition](ClickhouseInputPartition(null, 0))
    }

    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    require(lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
        s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val boundValueToString: Long => String =
      toBoundValueInWhereClause(_, partitioning.columnType, timeZoneId)
    val numPartitions =
      if ((upperBound - lowerBound) >= partitioning.numPartitions || /* check for overflow */
        (upperBound - lowerBound) < 0) {
        partitioning.numPartitions
      } else {
        logWarning("The number of partitions is reduced because the specified number of " +
          "partitions is less than the difference between upper bound and lower bound. " +
          s"Updated number of partitions: ${upperBound - lowerBound}; Input number of " +
          s"partitions: ${partitioning.numPartitions}; " +
          s"Lower bound: ${boundValueToString(lowerBound)}; " +
          s"Upper bound: ${boundValueToString(upperBound)}.")
        upperBound - lowerBound
      }
    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.
    val stride: Long = upperBound / numPartitions - lowerBound / numPartitions

    var i: Int = 0
    val column = partitioning.column
    var currentValue = lowerBound
    val ans = new ArrayBuffer[InputPartition]()
    while (i < numPartitions) {
      val lBoundValue = boundValueToString(currentValue)
      val lBound = if (i != 0) s"$column >= $lBoundValue" else null
      currentValue += stride
      val uBoundValue = boundValueToString(currentValue)
      val uBound = if (i != numPartitions - 1) s"$column < $uBoundValue" else null
      val whereClause =
        if (uBound == null) {
          lBound
        } else if (lBound == null) {
          s"$uBound or $column is null"
        } else {
          s"$lBound AND $uBound"
        }
      ans += ClickhouseInputPartition(whereClause, i)
      i = i + 1
    }
    val partitions = ans.toArray
    logInfo(s"Number of partitions: $numPartitions, WHERE clauses of these partitions: " +
      partitions.map(_.asInstanceOf[JDBCPartition].whereClause).mkString(", "))
    partitions
  }

  // Verify column name and type based on the JDBC resolved schema
  private def verifyAndGetNormalizedPartitionColumn(
                                                     schema: StructType,
                                                     columnName: String): (String, DataType) = {

    val column = schema.find(f => f.name == columnName).getOrElse {
      val maxNumToStringFields = SQLConf.get.maxToStringFields
      throw new RuntimeException(s"User-defined partition column $columnName not " +
        s"found in the JDBC relation: ${schema.simpleString(maxNumToStringFields)}")
    }

    column.dataType match {
      case _: NumericType | DateType | TimestampType =>
      case _ =>
        throw new RuntimeException(
          s"Partition column type should be ${NumericType.simpleString}, " +
            s"${DateType.catalogString}, or ${TimestampType.catalogString}, but " +
            s"${column.dataType.catalogString} found.")
    }
    (column.name, column.dataType)
  }

  private def toInternalBoundValue(
                                    value: String,
                                    columnType: DataType,
                                    timeZoneId: String): Long = {
    def parse[T](f: UTF8String => Option[T]): T = {
      f(UTF8String.fromString(value)).getOrElse {
        throw new IllegalArgumentException(
          s"Cannot parse the bound value $value as ${columnType.catalogString}")
      }
    }

    columnType match {
      case _: NumericType => value.toLong
      case DateType => parse(stringToDate(_, getZoneId(timeZoneId))).toLong
      case TimestampType => parse(stringToTimestamp(_, getZoneId(timeZoneId)))
    }
  }

  private def toBoundValueInWhereClause(
                                         value: Long,
                                         columnType: DataType,
                                         timeZoneId: String): String = {
    def dateTimeToString(): String = {
      val dateTimeStr = columnType match {
        case DateType =>
          val dateFormatter = DateFormatter(DateTimeUtils.getZoneId(timeZoneId))
          dateFormatter.format(value.toInt)
        case TimestampType =>
          val timestampFormatter = TimestampFormatter.getFractionFormatter(
            DateTimeUtils.getZoneId(timeZoneId))
          DateTimeUtils.timestampToString(timestampFormatter, value)
      }
      s"'$dateTimeStr'"
    }

    columnType match {
      case _: NumericType => value.toString
      case DateType | TimestampType => dateTimeToString()
    }
  }
}
