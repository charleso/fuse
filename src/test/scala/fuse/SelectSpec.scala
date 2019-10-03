package fuse

import hedgehog._
import hedgehog.runner._

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scalaz.Scalaz._

object SelectSpec extends Properties {

  override def tests: List[Test] =
    List(
      example("Basic test", test)
    )

  def test: Result = {
    val schema = StructType(List(
        StructField("a", IntegerType)
      , StructField("b", StringType)
    ))

    val df = SparkUtil.toDF(List(
      Row(10, "x")
    , Row(1, "y")
    , Row(5, "z")
    ), schema)

    val result = (
        Select.column[Int](min("a"))
    |@| Select.column[Int](max("a"))
    |@| Select.constant("foo")
    |@| Select.column[Long](count("b"))
    )((_, _, _, _))

    Result.all(List(
      result.head(df) ==== (1, 10, "foo", 3l).right
    , result.toList(df).map(_.headOption) ==== result.head(df).map(Some(_))
    ))
  }
}
