package fuse

import hedgehog._
import hedgehog.runner._

import org.apache.spark.sql.Row

import scalaz.Scalaz._

object RowParserSpec extends Properties {

  override def tests: List[Test] =
    List(
      example("Invalid index", testIndexOutOfBounds)
    )

  def testIndexOutOfBounds: Result =
    (for {
      _ <- RowParser.getAs[String]
      _ <- RowParser.getAs[Double]
    } yield ()).apply(Row("test")) ==== RowParserError.indexOutOfBounds(1).left
}
