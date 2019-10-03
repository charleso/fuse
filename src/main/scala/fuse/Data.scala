package fuse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.reflect.ClassTag

import scalaz._, Scalaz._

/**
 * This is our own wrapper around `Dataset` to avoid some of the pain and complexity around `Encoder`s
 * and custom types.
 *
 * https://stackoverflow.com/questions/36648128/how-to-store-custom-objects-in-dataset
 */
// NOTE: Must be serializable to function with Spark java serialization
class Data[A](val rows: Dataset[Row], val E: DataEncoder[A]) extends Serializable {

  def mapRow[B](f: Row => Row)(implicit E: DataEncoder[B]): Data[B] =
    new Data(rows.map(f)(RowEncoder(E.schema)), E)

  def mapFrom[B](f: Row => B)(implicit E: DataEncoder[B]): Data[B] =
    mapRow(r => E.createRow(f(r)))

  def map[B](f: A => B)(implicit EB: DataEncoder[B]): Data[B] =
    mapRow(a => EB.createRow(f(E.fromRowUnsafe(a))))

  def rdd(implicit T: ClassTag[A]): RDD[A] =
    rows.rdd.map(E.fromRowUnsafe)

  def toList: List[A] =
    rows.collect.toList.map(E.fromRowUnsafe)

  /**
   * NOTE: This will result in the entire row being loaded,
   * which can be slower than just filtering by a single column with `where`.
   */
  def filter(f: A => Boolean): Data[A] =
    new Data(rows.filter(r => f(E.fromRowUnsafe(r))), E)

  def where(condition: Column): Data[A] =
    new Data(rows.where(condition), E)

  def select[B](columns: List[Column])(implicit EB: DataEncoder[B]): Data[B] =
    Data.fromDataset(rows.select(columns: _*))
      // This is ideally a "compile" error, but isn't something the user should see
      .valueOr(e => sys.error(e.render))

  def as[B](implicit EB: DataEncoder[B]): Data[B] =
    select(EB.schema.fieldNames.map(col(_)).toList)(EB)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def joinLeftOuter[B](right: Data[B], on1: String, onRest: String*): Data[(A, Option[B])] = {

    val enc = new DataEncoder[(A, Option[B])] {

      override def schema: StructType =
        StructType(List(
          StructField("l", E.schema)
        , StructField("r", right.E.schema)
        ))

      override def createRow(a: (A, Option[B])): Row =
        Row(E.createRow(a._1), a._2.map(right.E.createRow).orNull)

      override def fromRow: RowParser[(A, Option[B])] = {
        import RowParser._
        (   getAs[Row].flatMapFail(E.fromRow.apply)
        |@| getAsOption[Row].flatMapFail(_.traverseU(right.E.fromRow.apply))
        )((_, _))
      }
    }

    def condition(c: String): Column =
      col("_1." + c) === col("_2." + c)

    Data.fromDatasetUnsafe(
      // Unfortunately this seems to be the "easiest" way to force the clean separate of the two sides
      // It would be nice if we could do the same thing without the extra "map"
      // I tried `alias`ing the datasets, but that doesn't really do anything.
      // The `joinWith` does something similar but with different, internal expression types (`Alias`, `GetFieldStruct`).
      // WARNING: The additional aliases are required for Spark 2.3 but not Spark 2.2
      rows.as("_1").joinWith(
        right.rows.as("_2")
      , onRest.foldLeft(condition(on1))((acc, on) => acc && condition(on))
      , "left_outer"
      ).map(r => Row(r._1, r._2))(RowEncoder(enc.schema))
    )(enc)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def crossJoin[B](right: Data[B], filter: Column): Data[(A, B)] = {

    val enc = new DataEncoder[(A, B)] {

      override def schema: StructType =
        StructType(List(
          StructField("l", E.schema)
        , StructField("m", right.E.schema)
        ))

      override def createRow(a: (A, B)): Row =
        Row(E.createRow(a._1), right.E.createRow(a._2))

      override def fromRow: RowParser[(A, B)] = {
        import RowParser._
        (   getAs[Row].flatMapFail(E.fromRow.apply)
        |@| getAs[Row].flatMapFail(right.E.fromRow.apply)
        )((_, _))
      }
    }

    Data.fromDatasetUnsafe(
      rows.as("l").joinWith(right.rows.as("m"), filter, "cross")
        .map(r => Row(r._1, r._2))(RowEncoder(enc.schema))
    )(enc)
  }

  def persistInMem(): Data[A] =
    new Data(rows.persist(), E)

  def persist()(storage: StorageLevel): Data[A] =
    new Data(rows.persist(storage), E)

  def unpersist(): Data[A] =
    new Data(rows.unpersist(), E)
}

object Data {

  type Validated[A] = SchemaErrors \/ A

  def fromDatasetUnsafe[A](rows: Dataset[Row])(implicit E: DataEncoder[A]): Data[A] =
    new Data(rows, E)

  def fromDataset[A](rows: Dataset[Row])(implicit E: DataEncoder[A]): Validated[Data[A]] =
    DataEncoder.validateSchema(E, rows.schema).as(new Data(rows, E))

  def fromRows(rows: Dataset[Row]): Data[Row] =
    new Data(rows, DataEncoder.rowEncoder(rows.schema))

  def withSchema[M[_]: Functor, A](rows: Dataset[Row], f: StructType => M[DataEncoder[A]]): M[Data[A]] =
    f(rows.schema).map(enc => new Data(rows, enc))

  def fromList[A](session: SparkSession, rows: List[A])(implicit E: DataEncoder[A]): Data[A] =
    new Data(session.createDataset(rows.map(E.createRow))(RowEncoder(E.schema)), E)
}

// NOTE: Must be serializable to function with Spark java serialization
trait DataEncoder[A] extends Serializable {

  def schema: StructType

  def createRow(a: A): Row

  def fromRow: RowParser[A]

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def fromRowUnsafe(row: Row): A =
    fromRow(row).valueOr(e => throw new RuntimeException(e.render))

  /** Create this row with our own schema, which is needed to use field names instead of index */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def row(values: Any*): Row =
    new GenericRowWithSchema(values.toArray, schema)
}

object DataEncoder {

  def apply[A : DataEncoder]: DataEncoder[A] =
    implicitly[DataEncoder[A]]

  def rowEncoder(schema1: StructType): DataEncoder[Row] =
    new DataEncoder[Row] {

      override def schema: StructType =
        schema1

      override def createRow(row: Row): Row =
        row

      override def fromRow: RowParser[Row] =
        RowParser.row
    }

  /**
   * Make sure the given schema is compatible with the given encoder
   */
  def validateSchema[A](enc: DataEncoder[A], schema: StructType): SchemaErrors \/ Unit =
    enc.schema.fields.toList.foldMap(sf => (for {
      o <- schema.fields.find(_.name === sf.name).toRightDisjunction(SchemaError.missingField(sf.name))
      _ <-
        if(Equal.equalA.equal(o.dataType, sf.dataType))
          ().right
        else
          SchemaError.invalidType(sf.name, sf.dataType, o.dataType).left
    } yield ()).validationNel).disjunction.leftMap(SchemaErrors(_))
}

case class SchemaErrors(errors: NonEmptyList[SchemaError]) {
  def render: String =
    errors.list.toList.map(_.render).mkString("\n")

  def add(error: SchemaError): SchemaErrors =
    copy(errors = error <:: errors)
}
object SchemaErrors {
  def create(error: SchemaError, others: SchemaError*): SchemaErrors =
    SchemaErrors(NonEmptyList(error, others: _*))
}

sealed trait SchemaError {
  def render: String = this match {
    case SchemaError.MissingField(name) =>
      s"Data missing field '$name'"
    case SchemaError.InvalidType(name, expected, actual) =>
      s"Type missmatch for field '$name'. Expecting ${expected.toString} but got ${actual.toString}"
  }
}
object SchemaError {
  case class MissingField(name: String) extends SchemaError
  case class InvalidType(name: String, expected: DataType, actual: DataType) extends SchemaError

  def missingField(name: String): SchemaError =
    MissingField(name)

  def invalidType(name: String, expected: DataType, actual: DataType): SchemaError =
    InvalidType(name, expected, actual)
}
