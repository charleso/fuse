package fuse

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import hedgehog._
import hedgehog.runner._

object InputOutputSpec extends Properties {

  override def tests: List[Test] =
    List(
      example("Save/Load parquet should be symmetrical (file)", testSaveLoadParquetFile)
    , example("Save/Load parquet should respect schema merging", testSchemaMergeParquet)
    , example("Save/Load csv with header should be symmetrical (file)", testSaveLoadCsvFileHeader)
    , example("Save/Load csv without header should be symmetrical (file)", testSaveLoadCsvFileNoHeader)
    , example("Reading missing input files is handled", testInputFileMissing)
    )

  def testSaveLoadParquetFile: Result =
    FileUtil.withTempDir(dir => {
      val file = RemotePath(dir.toString + "/x.parquet")
      val schema = StructType(List(
        StructField("a", StringType)
      ))
      val df1 = SparkUtil.toDF(List(Row("a")), schema)
      Output.save(SparkUtil.session, Output.ParquetFile(file), List("x" -> "y"), df1)
      val df2 =
        Input
          .load(SparkUtil.session, Input.ParquetFile(file, ParquetInputProperties.default))
          .valueOr(e => sys.error(e.toString))
      Result.all(List(
        df1.collect.toList ==== df2.select("a").collect.toList
      , df2.select("x").collect.toList ==== List(Row("y"))
      ))
    })

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def testSchemaMergeParquet: Result =
    FileUtil.withTempDir(dir => {
      val file = RemotePath(dir.toString + "x.parquet")
      val schema1 = StructType(List(StructField("id", LongType)))
      val schema2 = StructType(
        schema1.fields ++
          List(StructField("word", StringType))
      )
      val schema3 =
        StructType(
          schema2.fields ++
            List(StructField("dub", DoubleType))
        )

      val df1 = SparkUtil.toDF(List(Row(1001L), Row(1002L)), schema1)
      Output.save(SparkUtil.session, Output.ParquetFile(file), List("schema" -> "schema1"), df1)

      val df2 = SparkUtil.toDF(List(Row(1005L, "green")), schema2)
      Output.save(SparkUtil.session, Output.ParquetFile(file), List("schema" -> "schema2"), df2)

      val df3 = SparkUtil.toDF(List(Row(1010L, "blue", 0.005), Row(1011L, "red", -1010.008)), schema3)
      Output.save(SparkUtil.session, Output.ParquetFile(file), List("schema" -> "schema3"), df3)

      val propsMerge = ParquetInputProperties.default.copy(mergeSchema = MergeSchemaProperty.Merge)
      val dfMerged = Input.load(SparkUtil.session, Input.ParquetFile(file, propsMerge)).valueOr(e => sys.error(e.toString))

      val propsSchema = ParquetInputProperties.default.copy(schemaProp = SchemaProperty.provided(schema3))
      val dfProvided = Input.load(SparkUtil.session, Input.ParquetFile(file, propsSchema)).valueOr(e => sys.error(e.toString))

      val expected =
        List(
          Row(1001, null, null, "schema1")
        , Row(1002, null, null, "schema1")
        , Row(1005, "green", null, "schema2")
        , Row(1010, "blue", 0.005, "schema3")
        , Row(1011, "red", -1010.008, "schema3")
        )

      val expectedSchema = StructType(schema3.fields ++ List(StructField("schema", StringType)))

      Result.all(
        List(
          dfMerged.schema ==== expectedSchema
        , dfProvided.schema ==== expectedSchema
        , dfMerged.orderBy(col("id")).collect.toList ==== expected
        , dfProvided.orderBy(col("id")).collect.toList ==== expected
        )
      )

    })

  def testSaveLoadCsvFileHeader: Result =
    FileUtil.withTempDir(dir => {
      val file = RemotePath(dir.toString + "/x.csv")
      val schema =
        StructType(List(
          StructField("intcol", IntegerType)
        , StructField("strcol", StringType)
        , StructField("dobcol", DoubleType)
        ))
      val df1 =
        SparkUtil.toDF(List(
          Row(101, "blah", 1909.7101)
        , Row(-99, "burp", -0.000007)
        , Row(9876, "gulp", 1111010.000007)
        ), schema)
      val outProps = CsvOutputProperties(HeaderProperty.Present)
      val inProps = CsvInputProperties(HeaderProperty.Present, InferSchemaProperty.Provided(schema))
      Output.save(SparkUtil.session, Output.CsvFile(file, outProps), List("x" -> "y"), df1)
      val df2 = Input.load(SparkUtil.session, Input.CsvFile(file, inProps)).valueOr(e => sys.error(e.toString))
      df1.orderBy("intcol").collect.toList ====
        df2.select(schema.fields.map(c => col(c.name)): _*).orderBy("intcol").collect.toList
    })

  def testSaveLoadCsvFileNoHeader: Result =
    FileUtil.withTempDir(dir => {
      val file = RemotePath(dir.toString + "/x.csv")
      val schema =
        StructType(List(
          StructField("intcol", IntegerType)
        , StructField("strcol", StringType)
        , StructField("dobcol", DoubleType)
        ))
      val df1 =
        SparkUtil.toDF(List(
          Row(101, "blah", 1909.7101)
        , Row(-99, "burp", -0.000007)
        , Row(9876, "gulp", 1111010.000007)
        ), schema)
      val outProps = CsvOutputProperties(HeaderProperty.Absent)
      val inProps = CsvInputProperties(HeaderProperty.Absent, InferSchemaProperty.Infer)
      Output.save(SparkUtil.session, Output.CsvFile(file, outProps), List("x" -> "y"), df1)
      val df2 =
        Input
          .load(SparkUtil.session, Input.CsvFile(file, inProps))
          .valueOr(e => sys.error(e.toString))
          .withColumnRenamed("_c0", "intcol")
          .withColumnRenamed("_c1", "strcol")
          .withColumnRenamed("_c2", "dobcol")
      df1.orderBy("intcol").collect.toList ====
        df2.select(schema.fields.map(c => col(c.name)): _*).orderBy("intcol").collect.toList
    })

  def testInputFileMissing: Result = {
    val file = RemotePath("/x.parquet")
    val td = Input.load(SparkUtil.session, Input.ParquetFile(file, ParquetInputProperties.default))
    Result.assert(td.isLeft).log(td.toString)
  }
}
