package com.imgur.spark.jdbc2parquet

import spray.json._
import WriteType.WriteType

object ConfigurationTypes extends DefaultJsonProtocol {
  final case class Configuration(
    runLocal: Boolean,
    jdbcConnection: JDBCConnection,
    parquetOutputPath: String,
    partitions: Int,
    table: String,
    indexColumn: String,
    minIndex: Long,
    maxIndex: Long,
    schema: Seq[ColumnConfiguration]
  )

  final case class JDBCConnection(
    driverClass: String,
    connectionPath: String,
    user: String,
    password: String
  )

  final case class ColumnConfiguration(
    readColumn: String,
    writeColumn: Option[String],
    writeType: WriteType
  )

  implicit object ReadTypeFormat extends RootJsonFormat[WriteType.WriteType] {
    def write(readType: WriteType.WriteType): JsValue = return JsString(readType.toString)
    def read(readTypeJson: JsValue): WriteType.WriteType = readTypeJson match {
      case JsString(str) => WriteType.withName(str)
      case _ => throw new DeserializationException("Unexpected read type '%s'.".format(readTypeJson))
    }
  }
  implicit val JDBCConnectionFormat: JsonFormat[JDBCConnection] = jsonFormat4(JDBCConnection)
  implicit val ColumnConfigurationFormat: JsonFormat[ColumnConfiguration] = jsonFormat3(ColumnConfiguration)
  implicit val ConfigurationFormat: JsonFormat[Configuration] = lazyFormat(jsonFormat9(Configuration))
}