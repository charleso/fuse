package fuse

import hedgehog.runner._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

// FIXME: Having this global is horrible, but unfortunately spark gives us no choice.
trait SparkUtilCommon {

  val conf: SparkConf =
    new SparkConf().setAppName("Test").setMaster("local[2]").set("spark.sql.codegen.wholeStage", "false")

  def session: SparkSession

  // https://community.hortonworks.com/content/supportkb/223190/sparksession-is-unable-to-use-hive-metastorecatalo.html
  lazy val sc: SparkContext =
    session.sparkContext

  def toDF(l: List[Row], schema: StructType): DataFrame =
    session.createDataFrame(l.asJava, schema)

  def toRDD[A: ClassTag](l: List[A]): RDD[A] =
    session.sparkContext.parallelize[A](l)

  def prop(p: Prop): Prop =
    p.withTests(10)
       // FIXME Shrinking is currently broken for slow tests,
       // https://github.com/hedgehogqa/scala-hedgehog/issues/66
      .noShrinking

  def props(p: Prop, ps: Prop*): List[Test] =
    (p +: ps).map(SparkUtil.prop).toList
}
