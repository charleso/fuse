package fuse

import hedgehog._
import hedgehog.core.{ForAll, Name}

import scalaz._

object GenPlus {

  def assertWith[A](a: A)(f: A => Result): Result =
    f(a).log(a.toString)

  def assertWithExpected[A, B](actual: A, expected: B)(f: (A, B) => Result): Result =
    assertWithNamed2("actual", actual, "expected", expected)(f)

  def assertWithNamed2[A, B](aName: Name, a: A, bName: Name, b: B)(f: (A, B) => Result): Result =
    f(a, b)
      .log(s"${aName.value}: ${a.toString}\n  ${bName.value}: ${b.toString}")

  def assertWithNamed[A](name: Name, a: A)(f: A => Result): Result =
    f(a)
      .log(ForAll(name, a.toString))

  def assertApproxEqual(actual: Double, expected: Double, tolerance: Double): Result =
    assertWithExpected(actual, expected)((a, e) =>
      Result.assert(
        (a.isNaN && e.isNaN) ||
        (a.isPosInfinity && e.isPosInfinity) ||
        (a.isNegInfinity && e.isNegInfinity) ||
        Math.copySign(a - e, 1.0D) <= tolerance
      )
    )

  def genString: Gen[String] =
    Gen.string(Gen.unicode, Range.linear(0, 100))

  // FIX Move some/all of these to hedgehog extras?

  def genShort: Gen[Short] =
    Gen.short(Range.linearFrom(0, Short.MinValue, Short.MaxValue))

  def genPositiveShort: Gen[Short] =
    Gen.short(Range.linearFrom(1, 1, Short.MaxValue))

  def genInt: Gen[Int] =
    Gen.int(Range.linearFrom(0, Int.MinValue, Int.MaxValue))

  def genPositiveInt: Gen[Int] =
    Gen.int(Range.linearFrom(1, 1, Int.MaxValue))

  def genLong: Gen[Long] =
    Gen.long(Range.linearFrom(0, Long.MinValue, Long.MaxValue))

  def genNaturalLong: Gen[Long] =
    Gen.long(Range.linearFrom(0, 0, Long.MaxValue))

  def genFloat: Gen[Float] =
    Gen.double(Range.linearFracFrom(0, Float.MinValue, Float.MaxValue).map(_.toDouble)).map(_.toFloat)

  def genDouble: Gen[Double] =
    Gen.double(Range.linearFracFrom(0, Double.MinValue, Double.MaxValue))

  def genPositiveDouble: Gen[Double] =
    Gen.double(Range.linearFracFrom(0, 0.0, Double.MaxValue))

  def genNonEmptyList[A](g: Gen[A], r: Range[Int]): Gen[NonEmptyList[A]] = {
    for {
      h <- g
      t <- g.list(r)
    } yield NonEmptyList(h, t: _*)
  }

  // NOTE: This may return a NEL shorter than the range would suggest
  def genNELUniqShorterThanRange[A](g: Gen[A], r: Range[Int]): Gen[NonEmptyList[A]] = {
    for {
      h <- g
      t <- g.list(r).filter(a => ! a.contains(h))
      ut = t.toSet.toList
    } yield NonEmptyList(h, ut: _*)
  }

  def genMinMax: Gen[(Double, Double)] =
    for {
      min <- genDouble.simple
      max <- genDouble.simple
    } yield if (min < max) (min, max) else (max, min)

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def elementUnsafe[A](l: List[A]): Gen[A] =
    Gen.element(l.head, l.tail)

  implicit class GenSyntax[A](gen: Gen[A]) {
    /** All double values without NaN or infinity */
    def simple(implicit ev: A =:= Double): Gen[Double] =
      // https://github.com/argonaut-io/argonaut/blob/c3ac02677e2db6bb21cf5cc17e0445e1ba467473/argonaut/shared/src/main/scala/argonaut/Json.scala#L546-L552
      gen.map(ev).filter(d => !(d.isInfinite || d.isNaN))
  }

  def genSublistOf[A](f: List[A]): Gen[List[A]] =
    hedgehog.predef.sequence[Gen, (A, Boolean)](f.map(a => Gen.boolean.map(a -> _)))
      .map(_.filter(_._2).map(_._1))
}
