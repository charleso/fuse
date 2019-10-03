package fuse

import hedgehog._
import hedgehog.runner._

import scala.reflect.ClassTag

import scalaz.Scalaz._

object DataLaws {

  def laws[A](gen: Gen[A])(implicit T: ClassTag[A], enc: DataEncoder[A]): List[Test] =
    lawsNamed(T.runtimeClass.getName, gen)

  def lawsNamed[A](name: String, gen: Gen[A])(implicit enc: DataEncoder[A]): List[Test] =
    List(
      Prop(s"$name data roundtrip", testRoundTrip(gen, enc))
    )

  def lawsWithEncoder[A](gen: Gen[(A, DataEncoder[A])])(implicit T: ClassTag[A]): List[Test] =
    List(
      Prop(s"${T.runtimeClass.getName} data roundtrip",
        gen.lift.flatMap(g => testRoundTrip(Gen.constant(g._1), g._2))
      )
    )

  def testRoundTrip[A](gen: Gen[A], enc: DataEncoder[A]): Property =
    for {
      a <- gen.forAll
    } yield enc.fromRow(enc.createRow(a)) ==== a.right
}
