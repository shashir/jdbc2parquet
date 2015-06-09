package com.imgur.spark.jdbc2parquet

import java.sql.ResultSet

import ConfigurationTypes.ColumnConfiguration
import org.apache.spark.sql.Row
import WriteType.WriteType

import scala.annotation.tailrec

object RowReadUtils {
  def getResults(columnConfigurations: Seq[ColumnConfiguration])(resultSet: ResultSet): Seq[Row] = {
    @tailrec def _getResults(columnConfigurations: Seq[ColumnConfiguration])(
      resultSet: ResultSet,
      agg: Seq[Row]
      ): Seq[Row] = {
      if (resultSet.next()) {
        _getResults(columnConfigurations)(
          resultSet,
          agg.+:(Row.fromSeq(
            columnConfigurations.map { columnConfig: ColumnConfiguration =>
              resultSet.getReadTypeColumn(columnConfig.readColumn, columnConfig.writeType)
            }
          ))
        )
      } else {
        agg.reverse
      }
    }

    return _getResults(columnConfigurations)(resultSet, Seq())
  }

  implicit class ExtendedResultSet(resultSet: ResultSet) {
    def getNullablePrimitive[T](columnName: String, primitiveExtractor: ResultSet => T): T = {
      if (null == resultSet.getObject(columnName)) {
        return null.asInstanceOf[T]
      } else {
        return primitiveExtractor(resultSet)
      }
    }
    def getNullableEpoch(columnName: String): java.lang.Long = getNullablePrimitive[java.lang.Long](
      columnName,
      _.getTimestamp(columnName).getTime
    )
    def getNullableLong(columnName: String): java.lang.Long = getNullablePrimitive[java.lang.Long](
      columnName,
      _.getLong(columnName)
    )
    def getNullableInt(columnName: String): java.lang.Integer = getNullablePrimitive[java.lang.Integer](
      columnName,
      _.getInt(columnName)
    )
    def getNullableDouble(columnName: String): java.lang.Double = getNullablePrimitive[java.lang.Double](
      columnName,
      _.getDouble(columnName)
    )
    def getReadTypeColumn(columnName: String, readType: WriteType): Any = {
      return readType match {
        case WriteType.Int => getNullableInt(columnName)
        case WriteType.Long => getNullableLong(columnName)
        case WriteType.String => resultSet.getString(columnName)
        case WriteType.Double => getNullableDouble(columnName)
        case WriteType.Epoch => getNullableEpoch(columnName)
      }
    }
  }
}
