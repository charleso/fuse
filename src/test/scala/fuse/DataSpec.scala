package fuse

import hedgehog._
import hedgehog.runner._

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object DataSpec extends Properties {

  override def tests: List[Test] =
    SparkUtil.props(
      example("Left Outer Join", testJoinLeftOuter)
    , example("Cross Join", testCrossJoin)
    , example("Select", testSelect)
    , example("Select invalid schema", testSelectFail)
    , example("As", testAs)
    )

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def testJoinLeftOuter: Result = {
    val s1 = StructType(List(
        StructField("a", StringType)
      , StructField("b", StringType)
      , StructField("c", StringType)
    ))
    val s2 = StructType(List(
      StructField("b", StringType)
    , StructField("c", StringType)
    ))
    val l = SparkUtil.toDF(
      List(Row("1", "b", "x"), Row("2", "b", "x"), Row("3", "b", "y"), Row("4", "c", "y")), s1)
    val r = SparkUtil.toDF(List(Row("b", "x")), s2)
    val x = Data.fromRows(l).joinLeftOuter(Data.fromRows(r), "b", "c").toList
    x ==== List(
      (Row("1", "b", "x"), Some(Row("b", "x")))
    , (Row("2", "b", "x"), Some(Row("b", "x")))
    , (Row("3", "b", "y"), None)
    , (Row("4", "c", "y"), None)
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def testCrossJoin: Result = {
    val s1 = StructType(List(
      StructField("a", StringType)
    , StructField("b", StringType)
    ))
    val s2 = StructType(List(
      StructField("b", StringType)
    , StructField("c", StringType)
    ))
    val l = SparkUtil.toDF(List(Row("1", "b"), Row("2", "b")), s1)
    val r = SparkUtil.toDF(List(Row("b", "x")), s2)
    val x = Data.fromRows(l).crossJoin(Data.fromRows(r), lit(true)).toList
    x ==== List(
        (Row("1", "b"), Row("b", "x"))
      , (Row("2", "b"), Row("b", "x"))
      )
  }

  def testSelect: Result = {
    val s1 = StructType(List(
        StructField("a", StringType)
      , StructField("b", StringType)
    ))
    val s2 = StructType(List(
      StructField("b", StringType)
    ))
    val rows = SparkUtil.toDF(List(Row("1", "a"), Row("2", "b"), Row("3", "c")), s1)
    val x = Data.fromRows(rows).select(List(col("b")))(DataEncoder.rowEncoder(s2)).toList
    x ==== List(Row("a"), Row("b"), Row("c"))
  }

  def testSelectFail: Result = {
    val s1 = StructType(List(
        StructField("a", StringType)
      , StructField("b", StringType)
    ))
    val s2 = StructType(List(
      StructField("b", StringType)
    ))
    val rows = SparkUtil.toDF(Nil, s1)
    try {
      Data.fromRows(rows).select(List(col("a")))(DataEncoder.rowEncoder(s2)).toList
      Result.failure.log("Should not be able to select invalid columns")
    } catch {
      case _: Exception =>
        Result.success
    }
  }

  def testAs: Result = {
    val s1 = StructType(List(
        StructField("a", StringType)
      , StructField("b", StringType)
    ))
    val s2 = StructType(List(
      StructField("b", StringType)
    ))
    val rows = SparkUtil.toDF(List(Row("1", "v1"), Row("2", "v2"), Row("3", "v3")), s1)
    val x = Data.fromRows(rows).as(DataEncoder.rowEncoder(s2)).toList
    x ==== List(Row("v1"), Row("v2"), Row("v3"))
  }


}
