package org.apache.spark.clickhouse

import org.apache.spark.internal.Logging
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import ru.yandex.clickhouse.{ClickHouseDataSource, ClickHouseStatement}
import ru.yandex.clickhouse.settings.{ClickHouseProperties, ClickHouseQueryParam}

import java.sql.ResultSet
import java.util.Properties
import scala.sys.process._


trait ITSpec extends AnyFlatSpec with BeforeAndAfterAll with Logging {

  override protected def beforeAll(): Unit = {
    logInfo("Starting docker compose...")
    "docker-compose down" !!;
    "docker-compose up -d" !!
  }

  override protected def afterAll(): Unit = {
    "docker-compose down" !!
  }

  private def createClickhouseStatement(): ClickHouseStatement = {
    val props = new Properties()
    val clickHouseProps = new ClickHouseProperties(props)
    val url = "jdbc:clickhouse://localhost:8123"
    logDebug(s"JDBC Connection string: $url")
    val dataSource = new ClickHouseDataSource(url, clickHouseProps)
    val conn = dataSource.getConnection

    conn.createStatement()
  }

  def executeQuery(query: String): ResultSet = {
    val stmt = createClickhouseStatement()
    stmt.executeQuery(query)
  }
}
