package com.imgur.spark.jdbc2parquet

import com.imgur.spark.jdbc2parquet.ConfigurationTypes._
import com.imgur.spark.jdbc2parquet.SparkSQLUtils.SparkJDBCParams
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.clapper.argot.ArgotParser
import org.clapper.argot.ArgotConverters._
import spray.json._
import scala.io.Source

import org.apache.spark.sql.Row


object JDBC2Parquet extends App {
  val LOCAL_MASTER: String = "local"
  val UTF_8 = "UTF-8"
  val parser = new ArgotParser("jdbc2parquet")
  val etlConfigFileParser = parser.option[String](
    List("config", "c"),
    "configuration",
    "Configuration file path on local disk."
  )
  parser.parse(args)
  require(!etlConfigFileParser.value.isEmpty, "Provide config file with param --config")
  val etlConfigFileSource: Source = Source.fromFile(etlConfigFileParser.value.get, UTF_8)
  val etlConfigJson: String = etlConfigFileSource.getLines().mkString("\n")
  val etlConfig: Configuration = etlConfigJson.parseJson.convertTo[Configuration]
  etlConfigFileSource.close()

  val conf = new SparkConf().setAppName("%s:\n%s".format(this.getClass.getName, etlConfigJson.parseJson.prettyPrint))
  if (etlConfig.runLocal) {
    conf.setMaster(LOCAL_MASTER)
  }
  val spark = new SparkContext(conf)
  Class.forName(etlConfig.jdbcConnection.driverClass).newInstance

  val sparkJDBCParams: Broadcast[SparkJDBCParams] = spark.broadcast(SparkJDBCParams(
    etlConfig.jdbcConnection.connectionPath,
    etlConfig.jdbcConnection.user,
    etlConfig.jdbcConnection.password,
    SparkSQLUtils.formatQuery(
      etlConfig.table,
      etlConfig.indexColumn,
      etlConfig.schema
    ),
    etlConfig.partitions,
    etlConfig.minIndex,
    etlConfig.maxIndex,
    etlConfig.schema
  ))

  val rowFromJDBC: RDD[Row] = new JdbcRDD[Row](
    spark,
    sparkJDBCParams.value.getConnection,
    sparkJDBCParams.value.query,
    sparkJDBCParams.value.minIndex,
    sparkJDBCParams.value.maxIndex,
    sparkJDBCParams.value.partitions,
    RowReadUtils.getResults(sparkJDBCParams.value.schema)
  )

  val sparkSQL = new SQLContext(spark)
  val parquetRDD = sparkSQL.applySchema(rowFromJDBC, SparkSQLUtils.getSchema(etlConfig.schema))
  parquetRDD.saveAsParquetFile(etlConfig.parquetOutputPath)

  spark.stop()
}
