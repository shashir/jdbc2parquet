package com.imgur.spark.jdbc2parquet

object WriteType extends Enumeration {
  type WriteType = Value
  val Int, Long, Double, String, Epoch = Value
}
