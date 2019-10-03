package fuse

import fuse.GenPlus._

import hedgehog._
import hedgehog.predef.sequence

import java.sql.{Timestamp => JTimestamp}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object SparkGens {

  def genPath: Gen[RemotePath] = {
    // A simplified path. If this changes over time it's important that we never generate:
    // - Paths starting with "/"
    // - Paths containing ".."
    // Otherwise it's possible to generate something that resolves to "/" which has been known to delete someones filesystem.
    val gen = Gen.choice1(Gen.lower, Gen.digit)
    for {
      // A crude way to make sure we _never_ end up with two paths where one file is the directory path of another
      dirs <- Gen.string(gen, Range.linear(1, 10)).map(s => s + ".d").list(Range.linear(0, 9))
      file <- Gen.string(gen, Range.linear(1, 10)).filter(!_.endsWith(".d"))
    } yield RemotePath((file :: dirs).reverse.mkString("/"))
  }

  def genApplicationId: Gen[ApplicationId] =
    genString.map(ApplicationId(_))

  def genStorageLevel: Gen[StorageLevel] =
    Gen.element1(
      StorageLevel.NONE
    , StorageLevel.DISK_ONLY
    , StorageLevel.DISK_ONLY_2
    , StorageLevel.MEMORY_ONLY
    , StorageLevel.MEMORY_ONLY_2
    , StorageLevel.MEMORY_ONLY_SER
    , StorageLevel.MEMORY_ONLY_SER_2
    , StorageLevel.MEMORY_AND_DISK
    , StorageLevel.MEMORY_AND_DISK_2
    , StorageLevel.MEMORY_AND_DISK_SER
    , StorageLevel.MEMORY_AND_DISK_SER_2
    , StorageLevel.OFF_HEAP
    )

  def genSchema(maxDepth: Int): Gen[StructType] =
    (for {
      dt <- genDataType(maxDepth)
      n <- Gen.boolean
    } yield dt -> n)
      .list(Range.linear(1, 10))
      .map(_.zipWithIndex.map({ case ((dt, n), i) =>
        StructField(s"attr$i", dt, n)
      }))
      .map(StructType(_))

  def genArrayType(maxDepth: Int): Gen[ArrayType] =
    genDataType(maxDepth).map(dt => ArrayType(dt))

  def genDataType(maxDepth: Int): Gen[DataType] =
    if(maxDepth > 1)
      Gen.frequency1(
          8 -> genBasicType
        , 1 -> genArrayType(maxDepth - 1).map(t => t: DataType)
        , 1 -> genSchema(maxDepth - 1).map(t => t: DataType)
        )
    else
      genBasicType

  def genBasicType: Gen[DataType] =
    Gen.frequency1[DataType](
        5 -> Gen.constant(StringType)
      , 5 -> genNumericType
      , 1 -> Gen.constant(TimestampType)
      )

  def genNumericType[T >: NumericType]: Gen[T] =
    Gen.frequency1[T](
        5 -> Gen.constant(IntegerType)
      , 5 -> Gen.constant(LongType)
      , 5 -> Gen.constant(DoubleType)
      , 1 -> Gen.constant(ShortType)
      , 1 -> Gen.constant(FloatType)
      )

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def genRow(schema: StructType): Gen[Row] =
    sequence[Gen, Any](schema.fields.toList.map(sf =>
      Gen.frequency(
          9 -> genValue(sf.dataType)
        , if (sf.nullable) List(1 -> Gen.constant(null: Any)) else Nil
        )
    )).map(values => new catalyst.expressions.GenericRowWithSchema(values.toArray, schema))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def genValue(dataType: DataType): Gen[Any] =
    (dataType match {
      case _: StringType => genString
      case _: ShortType => genShort
      case _: IntegerType => genInt
      case _: LongType => genLong
      case _: FloatType => genFloat
      case _: DoubleType => genDouble.simple
      case _: TimestampType => genTimestamp
      case a: ArrayType => genValue(a.elementType).list(Range.linear(0, 5))
      case s: StructType => genRow(s)
      case o => sys.error(s"Unknown type $o")
    }).map(t => t: Any)

  // Using 253402261198000 which is 9999-12-31 23:59:58 because higher values start causing issues
  // in spark when serializing/deserializing it.
  // Note: the second is 58 and not 59 as 9999-12-31 23:59:59 is used as a special missing value
  def genInstant: Gen[java.time.Instant] =
    Gen.long(Range.linear(0, 253402261198000l)).map(java.time.Instant.ofEpochMilli)

  def genTimestamp: Gen[JTimestamp] =
    genInstant.map(JTimestamp.from)

  def genRows(schema: StructType): Gen[List[Row]] =
    genRow(schema).list(Range.linear(1, 100))

  def genTableName: Gen[TableName] =
    Gen.alpha.map(_.toString).map(TableName)

  def genTableInput: Gen[Input] =
    genTableName.map(Input.table)

  def genInput: Gen[Input] =
    Gen.choice1(genTableName.map(Input.table), genPath.map(i => Input.parquetFile(i, ParquetInputProperties.default)))
}
