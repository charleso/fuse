import scalaz._

package object fuse {

  type DataEncoder[A] = DataEncoderRaw[A, A]
  type Data[A] = DataRaw[A, A]

  implicit def DataTypeEqual: Equal[org.apache.spark.sql.types.DataType] =
    Equal.equalA
}
