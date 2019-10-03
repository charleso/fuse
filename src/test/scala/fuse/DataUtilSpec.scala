package fuse

import fuse.SparkGens._
import fuse.GenPlus._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import hedgehog._
import hedgehog.runner._

import scalaz._, Scalaz._


object DataUtilSpec extends Properties {

  override def tests: List[Test] =
    List(
      property("test withPersistRDD does persist RDD", testPersistRDDDoesPersist)
    , property("test withPersistData does persist Data", testPersistDataDoesPersist)
    , property("test withPersistDataFrame does persist DF", testPersistDataFrameDoesPersist)
    , property("test withPersistRDD works if we unpersist early", testPersistRDDidempotent)
    , property("test withPersistData works if we unpersist early", testPersistDataidempotent)
    , property("test withPersistDataFrame works if we unpersist early", testPersistDataFrameidempotent)
    , example("test withPersistRDD does use the default storage level", testPersistRDDDefault)
    , example("test withPersistData does use the default storage level", testPersistDataDefault)
    , example("test withPersistDataFrame does use the default storage level", testPersistDataFrameDefault)
    , example("test withPersistRDD fails if RDD already persisted with different storage level"
        ,testPersistRDDAlreadyCached)
    , example("test withPersistData does not fail if Data already persisted with different storage level"
        ,testPersistDataAlreadyCached)
    , example("test withPersistDataFrame does not fail if DF already persisted with different storage level"
        ,testPersistDataFrameAlreadyCached)
    )

  /*
   * NOTE: we will get warnings here if genStorageLevel picks a level with more than 1x replication
   * as there is only one executor in local mode and it can't replicate to a second peer
   */
  def testPersistRDDDoesPersist: Property =
    for {
      rdd <- genInt.list(Range.linear(0, 10)).map(SparkUtil.toRDD).log("RDD")
      levelIn <- genStorageLevel.log("storage level")
    } yield
      Result.all(
        List(
          DataUtil.withPersistRDD(rdd, Some(levelIn)) { r =>
            val _ = r.count
            r.getStorageLevel
          } ==== levelIn.right
        , Result.assert(DataUtil.isUnpersistedRDD(rdd)))
      )

  def testPersistDataDoesPersist: Property =
    for {
      data <- genDouble.list(Range.linear(0, 10)).map { l =>
        Data.fromDatasetUnsafe(
          SparkUtil.toDF(l.zipWithIndex.map(x => Row(x._2, x._1)), idDoubleSchema)
        )(DataEncoder.rowEncoder(idDoubleSchema))
      }.log("data")
      levelIn <- genStorageLevel.log("storage level")
    } yield
      Result.all(
        List(
          DataUtil.withPersistData(data, Some(levelIn)) { d =>
            val _ = d.rows.count
            d.rows.storageLevel
          } ==== levelIn
          , Result.assert(DataUtil.isUnpersistedData(data))
        )
      )

  def testPersistDataFrameDoesPersist: Property =
    for {
      df <- genDouble.list(Range.linear(0, 10)).map(l =>
        SparkUtil.toDF(l.zipWithIndex.map(x => Row(x._2, x._1)), idDoubleSchema)
      ).log("df")
      levelIn <- genStorageLevel.log("storage level")
    } yield
      Result.all(
        List(
          DataUtil.withPersistDataFrame(df, Some(levelIn)) { d =>
            val _ = df.count
            df.storageLevel
          } ==== levelIn
          , Result.assert(DataUtil.isUnpersistedDataFrame(df))
        )
      )

  def testPersistRDDidempotent: Property =
    for {
      rdd <- genLong.list(Range.linear(0, 100)).map(SparkUtil.toRDD).log("RDD")
      levelIn <- genStorageLevel.log("storage level")
      _ = DataUtil.withPersistRDD(rdd, Some(levelIn)) { r =>
        val _ = r.count
        r.unpersist(true)
      }
    } yield Result.assert(DataUtil.isUnpersistedRDD(rdd))

  def testPersistDataidempotent: Property =
    for {
      data <- genLong.list(Range.linear(0, 10)).map { l =>
        Data.fromDatasetUnsafe(
          SparkUtil.toDF(l.zipWithIndex.map(x => Row(x._2, x._1)), idLongSchema)
        )(DataEncoder.rowEncoder(idLongSchema))
      }.log("data")
      levelIn <- genStorageLevel.log("storage level")
      _ = DataUtil.withPersistData(data, Some(levelIn)) { d =>
        val _ = d.rows.count
        d.rows.unpersist(true)
      }
    } yield Result.assert(DataUtil.isUnpersistedData(data))

  def testPersistDataFrameidempotent: Property =
    for {
      df <- genLong.list(Range.linear(0, 10)).map(l =>
        SparkUtil.toDF(l.zipWithIndex.map(x => Row(x._2, x._1)), idLongSchema)
      ).log("df")
      levelIn <- genStorageLevel.log("storage level")
      _ = DataUtil.withPersistDataFrame(df, Some(levelIn)) { d =>
        val _ = d.count
        d.unpersist(true)
      }
    } yield Result.assert(DataUtil.isUnpersistedDataFrame(df))

  /*
   * The default for RDDs is memory only:
   *
   * https://github.com/apache/spark/blob/5264164a67df498b73facae207eda12ee133be7d/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L202
   */
  def testPersistRDDDefault: Result = {
    val rdd = SparkUtil.toRDD((1 to 100).map(_.toDouble).toList)
    DataUtil.withPersistRDDDefault(rdd) { r =>
      val _ = r.count
      r.getStorageLevel
    } ==== StorageLevel.MEMORY_ONLY.right
  }

  def testPersistDataDefault: Result = {
    val data = Data.fromDatasetUnsafe(
      SparkUtil
        .toDF(
          (1 to 100).map(_.toDouble).zipWithIndex.map(x => Row(x._2, x._1)).toList
        , idDoubleSchema
        )
    )(DataEncoder.rowEncoder(idDoubleSchema))

    DataUtil.withPersistDataDefault(data) { d =>
      val _ = d.rows.count
      d.rows.storageLevel
    } ==== StorageLevel.MEMORY_AND_DISK
  }

  def testPersistDataFrameDefault: Result = {
    val df = SparkUtil.toDF(
      (1 to 100).map(_.toDouble).zipWithIndex.map(x => Row(x._2, x._1)).toList
      , idDoubleSchema
    )

    DataUtil.withPersistDataFrameDefault(df) { d =>
      val _ = d.count
      d.storageLevel
    } ==== StorageLevel.MEMORY_AND_DISK
  }

  def testPersistRDDAlreadyCached: Result = {
    val rdd = SparkUtil.toRDD((1 to 100).map(_.toDouble).toList)
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    DataUtil.withPersistRDD(rdd, Some(StorageLevel.MEMORY_ONLY))(_.count) ====
      PersistError
        .UnsupportedOperation("Cannot change storage level of an RDD after it was already assigned a level")
        .left
  }


  /*
   * A Dataset/frame won't cause an exception here but rather will ignore the call to re-cache
   * the data and log a warning:
   *
   * WARN execution.CacheManager: Asked to cache already cached data.
   */
  def testPersistDataAlreadyCached: Result = {
    val data = Data.fromDatasetUnsafe(
      SparkUtil.toDF((1 to 100).map(_.toLong).zipWithIndex.map(x => Row(x._2, x._1)).toList, idLongSchema)
    )(DataEncoder.rowEncoder(idLongSchema))

    val pdata = data.persist()(StorageLevel.MEMORY_AND_DISK)
    DataUtil.withPersistData(pdata, Some(StorageLevel.MEMORY_ONLY)) { d =>
      val _ = d.rows.count
      d.rows.storageLevel
    } ==== StorageLevel.MEMORY_AND_DISK
  }

  def testPersistDataFrameAlreadyCached: Result = {
    val df = SparkUtil.toDF((1 to 100).map(_.toLong).zipWithIndex.map(x => Row(x._2, x._1)).toList, idLongSchema)

    df.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    DataUtil.withPersistDataFrame(df, Some(StorageLevel.MEMORY_ONLY)) { d =>
      val _ = d.count
      d.storageLevel
    } ==== StorageLevel.MEMORY_AND_DISK_SER_2
  }

  val idDoubleSchema: StructType = StructType(
    StructField("id", IntegerType, nullable = true)
      :: StructField("value", DoubleType, nullable = true)
      :: Nil)

  val idLongSchema: StructType = StructType(
    StructField("id", IntegerType, nullable = true)
      :: StructField("value", LongType, nullable = true)
      :: Nil)

}
