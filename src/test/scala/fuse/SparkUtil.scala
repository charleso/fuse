package fuse

import org.apache.spark.sql.SparkSession

// FIXME: Having this global is horrible, but unfortunately spark gives us no choice.
object SparkUtil extends SparkUtilCommon {

  val session: SparkSession =
     SparkSession.builder
       .config(conf)
       .getOrCreate

}
