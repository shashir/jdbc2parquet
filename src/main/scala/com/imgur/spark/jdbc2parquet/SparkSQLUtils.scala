package com.imgur.spark.jdbc2parquet

import java.sql._

import com.imgur.spark.jdbc2parquet.ConfigurationTypes.ColumnConfiguration
import org.apache.spark.sql.StructType
import org.apache.spark.sql.StructField
import org.apache.spark.sql.IntegerType
import org.apache.spark.sql.DoubleType
import org.apache.spark.sql.LongType
import org.apache.spark.sql.StringType

object SparkSQLUtils {
  val QUERY_FORMAT: String =
  """
    |SELECT %s
    |FROM %s
    |WHERE %s >= ? AND %s <= ?
  """.stripMargin

  def assembleColumnsString(columnConfigurations: Seq[ColumnConfiguration]): String = {
    return columnConfigurations.map { columnConfiguration: ColumnConfiguration =>
      columnConfiguration.readColumn
    }.reduce(_ + ",\n" + _)
  }

  def formatQuery(
    table: String,
    indexColumn: String,
    columnConfigurations: Seq[ColumnConfiguration]
  ): String = {
    return QUERY_FORMAT.format(
      assembleColumnsString(columnConfigurations),
      table,
      indexColumn,
      indexColumn
    )
  }

  def getSchema(columnConfigurations: Seq[ColumnConfiguration]): StructType = {
    return StructType(
      columnConfigurations.map { columnConfiguration: ColumnConfiguration =>
        StructField(
          columnConfiguration.writeColumn.getOrElse(columnConfiguration.readColumn),
          columnConfiguration.writeType match {
            case WriteType.Int => IntegerType
            case WriteType.Long => LongType
            case WriteType.Double => DoubleType
            case WriteType.Epoch => LongType
            case WriteType.String => StringType
          },
          true
        )
      }
    )
  }

  final case class SparkJDBCParams(
    jdbcConnectionPath: String,
    user: String,
    password: String,
    query: String,
    partitions: Int,
    minIndex: Long,
    maxIndex: Long,
    schema: Seq[ColumnConfiguration]
  ) {
    def getConnection(): Connection = {
      DriverManager.getConnection(
        jdbcConnectionPath,
        user,
        password
      )
    }
  }
}
