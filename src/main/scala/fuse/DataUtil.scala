package fuse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scalaz._, Scalaz._

object DataUtil {

  implicit def StorageLevelEquals: Equal[StorageLevel] =
    Equal.equalA[StorageLevel]

  def withPersistRDD[A, B](rdd: RDD[A], sl: Option[StorageLevel])(f: RDD[A] => B): PersistError \/ B =
    try {
      sl match {
        case Some(level) =>
          rdd.persist(level)
        case None =>
          rdd.persist()
      }
      f(rdd).right
    } catch {
      case uoe: UnsupportedOperationException =>
        PersistError.unsupportedOperation(uoe.getMessage).left
    } finally {
      val _ = rdd.unpersist()
    }

  /*
   * RDD default is StorageLevel.MEMORY_ONLY
   */
  def withPersistRDDDefault[A, B](rdd: RDD[A])(f: RDD[A] => B): PersistError \/ B =
    withPersistRDD(rdd, None)(f)

  def isUnpersistedRDD[A](rdd: RDD[A]): Boolean =
    rdd.getStorageLevel === StorageLevel.NONE

  // not currently aware of any exceptions arising directly from Dataset.persist()
  def withPersistData[A, B](data: Data[A], sl: Option[StorageLevel])(f: Data[A] => B): B =
    withPersist(data.rows, sl)(f(data))

  /*
   * Dataset/frame default is StorageLevel.MEMORY_AND_DISK
   */
  def withPersistDataDefault[A, B](data: Data[A])(f: Data[A] => B): B =
    withPersist(data.rows, None)(f(data))

  def isUnpersistedData[A](data: Data[A]): Boolean =
    isUnpersistedDataFrame(data.rows)

  def withPersistDataFrame[B](df: DataFrame, sl: Option[StorageLevel])(f: DataFrame => B): B =
    withPersist(df, sl)(f(df))

  def withPersistDataFrameDefault[B](df: DataFrame)(f: DataFrame => B): B =
    withPersist(df, None)(f(df))

  def isUnpersistedDataFrame(df: DataFrame): Boolean =
    df.storageLevel === StorageLevel.NONE

  def withPersist[B](df: DataFrame, sl: Option[StorageLevel])(f: => B): B = {
    try {
      sl match {
        case Some(level) =>
          df.persist(level)
        case None =>
          df.persist()
      }
      f
    } finally {
      val _ = df.unpersist()
    }
  }

}

sealed trait PersistError

object PersistError {
  case class UnsupportedOperation(msg: String) extends PersistError

  def unsupportedOperation(msg: String): PersistError =
    UnsupportedOperation(msg)
}
