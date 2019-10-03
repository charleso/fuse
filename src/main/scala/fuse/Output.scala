package fuse

import org.apache.spark.sql.{DataFrame, DataFrameWriter, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

sealed trait Output

object Output {
  case class ParquetFile(file: RemotePath) extends Output

  case class CsvFile(file: RemotePath, props: CsvOutputProperties) extends Output

  case class Table(table: TableName) extends Output

  def parquetFile(file: RemotePath): Output =
    ParquetFile(file)

  def table(table: TableName): Output =
    Table(table)

  def save(session: SparkSession, out: Output, partitions: List[(String, String)], data: DataFrame): Unit =
    out match {
      case Output.ParquetFile(file) =>
        partitions
          .foldLeft(data)((d, y) => d.withColumn(y._1, lit(y._2)))
          .write
          .mode(SaveMode.Append)
          .partitionBy(partitions.map(_._1): _*)
          .parquet(file.value)
      case Output.CsvFile(file, props) =>
        CsvOutputProperties.writer(
          partitions
            .foldLeft(data)((d, y) => d.withColumn(y._1, lit(y._2)))
            .write
            .mode(SaveMode.Append)
            .partitionBy(partitions.map(_._1): _*)
        , props
        ).csv(file.value)
      case Output.Table(table) =>
        val partitionString =
          if (partitions.isEmpty)
            // partition() is invalid hive sql syntax unfortunately
            ""
          else
            "partition(" + partitions.map(x => x._1 + "='" + x._2 + "'").mkString(", ") + ")"
        val tmpTable = "temp_table"
        data.createOrReplaceTempView(tmpTable)
        // FIXME This is horrible, there _has_ to be a better way to set non-data based partition keys via the API
        // https://www.cloudera.com/documentation/enterprise/release-notes/topics/cdh_rn_spark_ki.html#ki_sparksql_dataframe_saveastable
        session.sql(s"""
          insert into ${table.render}
          $partitionString
          select * from $tmpTable
        """)
        ()
    }
}

/*
 * Many more we can add later: https://docs.databricks.com/spark/latest/data-sources/read-csv.html
 */
case class CsvOutputProperties(header: HeaderProperty)

object CsvOutputProperties {

  def writer[A](dfw: DataFrameWriter[A], props: CsvOutputProperties): DataFrameWriter[A] =
    HeaderProperty.writer(dfw, props.header)

}
