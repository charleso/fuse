import scalaz._

package object fuse {

  implicit def DataTypeEqual: Equal[org.apache.spark.sql.types.DataType] =
    Equal.equalA
}
