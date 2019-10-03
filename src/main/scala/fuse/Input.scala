package fuse

import org.apache.spark.sql.{AnalysisException, DataFrame, DataFrameReader, DataFrameWriter, SparkSession}
import org.apache.spark.sql.types.StructType

import scalaz._, Scalaz._

sealed trait Input

object Input {
  case class ParquetFile(file: RemotePath, props: ParquetInputProperties) extends Input

  case class CsvFile(file: RemotePath, props: CsvInputProperties) extends Input

  case class Table(table: TableName) extends Input

  def parquetFile(file: RemotePath, props: ParquetInputProperties): Input =
    ParquetFile(file, props)

  def csvFile(file: RemotePath, props: CsvInputProperties): Input =
    CsvFile(file, props)

  def table(table: TableName): Input =
    Table(table)

  def load(session: SparkSession, input: Input): InputLoadError \/ DataFrame =
    loadWithMetadata(session, input).map(_.data)

  def loadWithMetadata(session: SparkSession, input: Input): InputLoadError \/ InputData =
    input match {
      case Input.Table(inputTable) =>
        // *** WARNING **
        // The spark/hive integration is unfortunately not quite seamless.
        // In particular when it comes to case-sensitive there is some magic we have to be careful of.
        // Hive is case insensitive and so (by default in 2.2+) Spark will try to detect the schema from the parquet files
        // and then **update hive metadata**. Because we aren't using Spark the way it was quite intended,
        // without the following call we are bypassing this mechanism which results in _very_ inconsistent behaviour.
        // For now we are going to force Spark to update the cached schema until we have a safer/better way of relying on
        // a schema (or forcing case insensitivity).
        //
        // - https://medium.com/@an_chee/why-using-mixed-case-field-names-in-hive-spark-sql-is-a-bad-idea-95da8b6ec1e0
        // - https://issues.apache.org/jira/browse/SPARK-25391
        //
        // *** NOTE *** Please do NOT inline the variable below, the call to `loadTable` will then load the wrong schema
        val df = session.read.table(inputTable.render)

        InputData(InputMetadata.table(loadTable(session, inputTable)), df).right
      case Input.ParquetFile(baseDir, props) =>
        try {
          val df = ParquetInputProperties.reader(session.read, props).parquet(baseDir.value)
          InputData(InputMetadata.parquetFile(baseDir, df.schema, props), df).right
        } catch {
          case e: AnalysisException =>
            InputLoadError.file(e).left
        }
      case Input.CsvFile(baseDir, props) =>
        try {
          val df = CsvInputProperties.reader(session.read, props).csv(baseDir.value)
          InputData(InputMetadata.csvFile(baseDir, df.schema, props), df).right
        } catch {
          case e: AnalysisException =>
            InputLoadError.file(e).left
        }
    }

  def loadTable(session: SparkSession, tableName: TableName): TableDefinition = {
    val table = session.sessionState.catalog.getTableMetadata(
      session.sessionState.sqlParser.parseTableIdentifier(tableName.render)
    )
    TableDefinition(RemotePath(table.location.toString), table)
  }

  def loadData[A](session: SparkSession, input: Input)(implicit E: DataEncoder[A]): InputLoadDataError \/ Data[A] =
    loadDataWithSchema(session, input)(_ => E)

  def loadDataWithSchema[A](session: SparkSession, input: Input)(E: StructType => DataEncoder[A]): InputLoadDataError \/ Data[A] =
    for {
      d <- load(session, input).leftMap(InputLoadDataError.load)
      x <- Data.fromDataset[A](d)(E(d.schema)).leftMap(InputLoadDataError.schema)
    } yield x
}

/*
 * there are many more we can add if required: https://docs.databricks.com/spark/latest/data-sources/read-csv.html
 */
case class CsvInputProperties(
     header: HeaderProperty
   , inferSchema: InferSchemaProperty
   )

/*
 * None of the DataFrameReader stuff will blow up until we call .csv() at the end so we catch that in the try in
 * loadWithMetadata()
 */
object CsvInputProperties {
  def reader(dfr: DataFrameReader, props: CsvInputProperties): DataFrameReader =
    HeaderProperty.reader(
      InferSchemaProperty.reader(dfr, props.inferSchema)
    , props.header
    )
}

/*
 * Many more to add: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#configuration
 */
case class ParquetInputProperties(
     mergeSchema: MergeSchemaProperty
   , schemaProp: SchemaProperty
   )

object ParquetInputProperties {
  def default: ParquetInputProperties =
    ParquetInputProperties(
      MergeSchemaProperty.default
    , SchemaProperty.default
    )
  def reader(dfr: DataFrameReader, props: ParquetInputProperties): DataFrameReader =
    MergeSchemaProperty.reader(
      SchemaProperty.reader(dfr, props.schemaProp)
    , props.mergeSchema
    )
}

sealed trait HeaderProperty

object HeaderProperty {
  case object Present extends HeaderProperty
  case object Absent extends HeaderProperty

  def present: HeaderProperty = Present

  def absent: HeaderProperty = Absent

  val default: HeaderProperty =
    HeaderProperty.Absent

  def fromString(headerProperty: String): Option[HeaderProperty] =
    headerProperty match {
      case "present" => present.some
      case "absent" => absent.some
      case _ => none
    }

  def reader(dfr: DataFrameReader, header: HeaderProperty): DataFrameReader =
    header match {
      case Present =>
        dfr.option("header", "true")
      case Absent =>
        dfr.option("header", "false")
    }

  def writer[A](dfw: DataFrameWriter[A], header: HeaderProperty): DataFrameWriter[A] =
    header match {
      case Present =>
        dfw.option("header", "true")
      case Absent =>
        dfw.option("header", "false")
    }
}

sealed trait InferSchemaProperty

object InferSchemaProperty {
  case object Infer extends InferSchemaProperty
  case class Provided(schema: StructType) extends InferSchemaProperty

  def infer: InferSchemaProperty = Infer

  def default(schema: StructType): InferSchemaProperty =
    InferSchemaProperty.provided(schema)

  def provided(schema: StructType): InferSchemaProperty =
    Provided(schema)

  def reader(dfr: DataFrameReader, is: InferSchemaProperty): DataFrameReader =
    is match {
      case Infer =>
        dfr.option("inferSchema", "true")
      case Provided(schema) =>
        dfr.option("inferSchema", "false").schema(schema)
    }
}

/*
 * This covers cases like Parquet where there is no explicit 'inferSchema` property
 */
sealed trait SchemaProperty

object SchemaProperty {
  case object FromFiles extends SchemaProperty
  case class Provided(schema: StructType) extends SchemaProperty

  def fromFiles: SchemaProperty =
    FromFiles

  def provided(schema: StructType): SchemaProperty =
    Provided(schema)

  def default: SchemaProperty =
    SchemaProperty.FromFiles

  def reader(dfr: DataFrameReader, sp: SchemaProperty): DataFrameReader =
    sp match {
      case FromFiles =>
        dfr
      case Provided(schema) =>
        dfr.schema(schema)
    }
}

/*
 * Details of schema merging for Parquet:
 *  https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#schema-merging
 */
sealed trait MergeSchemaProperty

object MergeSchemaProperty {
  case object Merge extends MergeSchemaProperty
  case object DontMerge extends MergeSchemaProperty

  def merge: MergeSchemaProperty =
    Merge

  def dontMerge: MergeSchemaProperty =
    DontMerge

  def default: MergeSchemaProperty =
    MergeSchemaProperty.DontMerge

  def reader(dfr: DataFrameReader, m: MergeSchemaProperty): DataFrameReader =
    m match {
      case Merge =>
        dfr.option("mergeSchema", "true")
      case DontMerge =>
        dfr.option("mergeSchema", "false")
    }
}

sealed trait InputMetadata

object InputMetadata {

  case class ParquetFile(f: RemotePath, schema: StructType, props: ParquetInputProperties) extends InputMetadata

  case class CsvFile(f: RemotePath, schema: StructType, props: CsvInputProperties) extends InputMetadata

  case class Table(d: TableDefinition) extends InputMetadata

  def parquetFile(f: RemotePath, s: StructType, p: ParquetInputProperties): InputMetadata =
    ParquetFile(f, s, p)

  def csvFile(f: RemotePath, s: StructType, p: CsvInputProperties): InputMetadata =
    CsvFile(f, s, p)

  def table(d: TableDefinition): InputMetadata =
    Table(d)

  def rootPath(m: InputMetadata): RemotePath =
    m match {
      case ParquetFile(r, _, _) =>
        r
      case CsvFile(r, _, _) =>
        r
      case Table(d) =>
        d.path
    }

  def toInput(meta: InputMetadata): Input =
    meta match {
      case ParquetFile(p, _, props) =>
        Input.parquetFile(p, props)
      case CsvFile(path, _, props) =>
        Input.csvFile(path, props)
      case Table(TableDefinition(_, t)) =>
        Input.table(TableName(t.identifier.unquotedString))
    }

  def schema(meta: InputMetadata): StructType =
    meta match {
      case ParquetFile(_, s, _) =>
        s
      case CsvFile(_, s, _) =>
        s
      case Table(TableDefinition(_, t)) =>
        t.schema
    }
}

case class InputData(meta: InputMetadata, data: DataFrame)

sealed trait InputLoadError

object InputLoadError {

  case class File(e: AnalysisException) extends InputLoadError

  def file(e: AnalysisException): InputLoadError =
    File(e)
}

sealed trait InputLoadDataError

object InputLoadDataError {

  case class Load(e: InputLoadError) extends InputLoadDataError
  case class Schema(e: SchemaErrors) extends InputLoadDataError

  def load(e: InputLoadError): InputLoadDataError =
    Load(e)

  def schema(e: SchemaErrors): InputLoadDataError =
    Schema(e)
}
