name := "spark-clickhouse-connector"

version := "0.1"

scalaVersion := "2.12.12"

lazy val versions = new {
  val spark = "3.0.1"
  val clickhouseJdbc = "0.2.4"
  val scalaTest = "3.2.2"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions.spark % Provided,
  "org.apache.spark" %% "spark-sql" % versions.spark % Provided,
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % versions.clickhouseJdbc excludeAll(
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "net.jpountz.lz4")),

  "org.scalatest" %% "scalatest" % versions.scalaTest % Test
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", xs@_*) => MergeStrategy.first
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.last
}
assemblyJarName in assembly := "spark-clickhouse-connector.jar"