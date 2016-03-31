[DEPRECATED] jdbc2parquet
===

Use Spark to dump SQL tables into Parquet files (locally or on HDFS).

As of Spark 1.5(?), use JDBCRDD: https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala

E.g.

```scala
sqlContext
    .read.jdbc(url, "table", properties)
    .write.parquet("/path/to/output")
```
