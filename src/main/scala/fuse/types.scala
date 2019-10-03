package fuse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.types._

import scalaz._, Scalaz._

case class ApplicationId(render: String) extends AnyVal

object ApplicationId {

  def fromSession(session: SparkSession): ApplicationId =
    ApplicationId(session.sparkContext.applicationId)
}

case class TableName(render: String) extends AnyVal

object TableId {
  def fromTableName(table: TableName): TableIdentifierError \/ TableIdentifier =
    table.render.split('.').toList match {
      case tname :: Nil =>
        TableIdentifier(tname, None).right
      case db :: tname :: Nil =>
        TableIdentifier(tname, Some(db)).right
      case _ =>
        TableIdentifierError.invalidFormat(table).left
    }
}

case class TableDefinition(
    path: RemotePath
  , catalog: CatalogTable
  )

/**
 * This ties a spark DataType to an A. The reason we want to do this is so we can gaurantee type
 * parameters are compatible with spark DataTypes.
 */
class SparkDataType[A] private (val dataType: DataType)

object SparkDataType {

  def string: SparkDataType[String] =
    new SparkDataType(StringType)

  def short: SparkDataType[Short] =
    new SparkDataType(ShortType)

  def int: SparkDataType[Int] =
    new SparkDataType(IntegerType)

  def long: SparkDataType[Long] =
    new SparkDataType(LongType)

  def float: SparkDataType[Float] =
    new SparkDataType(FloatType)

  def double: SparkDataType[Double] =
    new SparkDataType(DoubleType)

  def byteArray: SparkDataType[Array[Byte]] =
    new SparkDataType(BinaryType)

  def list[A](dataType: SparkDataType[A]): SparkDataType[List[A]] =
    new SparkDataType(ArrayType(dataType.dataType))

  def unsafe[A](dataType: DataType): SparkDataType[A] =
    new SparkDataType(dataType)
}

sealed trait TableIdentifierError

object TableIdentifierError {
  case class InvalidFormat(table: TableName) extends TableIdentifierError

  def invalidFormat(table: TableName): TableIdentifierError =
    InvalidFormat(table)
}
