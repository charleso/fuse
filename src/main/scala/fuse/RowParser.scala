package fuse

import org.apache.spark.sql.Row

import scalaz._, Scalaz._

trait RowParser[A] { self =>

  def parse(index: Int, row: Row): RowParserError \/ (Int, A)

  final def apply(row: Row): RowParserError \/ A =
    parse(0, row).map(_._2)

  def map[B](f: A => B): RowParser[B] =
    new RowParser[B] {
      def parse(index: Int, row: Row): RowParserError \/ (Int, B) =
        self.parse(index, row).map(_.map(f))
    }

  def flatMap[B](f: A => RowParser[B]): RowParser[B] =
    new RowParser[B] {
      def parse(index: Int, row: Row): RowParserError \/ (Int, B) =
        self.parse(index, row) match {
          case -\/(e) =>
            e.left
          case \/-((i, a)) =>
            f(a).parse(i, row)
        }
    }

  def flatMapFail[B](f: A => RowParserError \/ B): RowParser[B] =
    self.flatMap(r => f(r).fold(RowParser.fail, RowParser.point(_)))

}

object RowParser {

  implicit def RowParserMonad: Monad[RowParser] =
    new Monad[RowParser] {

      override def point[A](a: => A): RowParser[A] =
        RowParser.point(a)

      override def bind[A, B](fa: RowParser[A])(f: A => RowParser[B]): RowParser[B] =
        fa.flatMap(f)
    }

  def fail[A](e: RowParserError): RowParser[A] =
    new RowParser[A] {
      def parse(_i: Int, _row: Row): RowParserError \/ (Int, A) =
        e.left
    }

  def point[A](a: => A): RowParser[A] =
    new RowParser[A] {
      def parse(i: Int, _row: Row): RowParserError \/ (Int, A) =
        (i, a).right
    }

  def row: RowParser[Row] =
    new RowParser[Row] {
      def parse(i: Int, row: Row): RowParserError \/ (Int, Row) =
        (i, row).right
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def getAs[A]: RowParser[A] =
    new RowParser[A] {
      def parse(i: Int, row: Row): RowParserError \/ (Int, A) =
        if (i < 0 || i >= row.length) {
          RowParserError.indexOutOfBounds(i).left
        } else {
          try {
            val a = row.get(i)
            (i + 1, a.asInstanceOf[A]).right
          } catch {
            case _: ClassCastException =>
              // FIXME This doesn't appear to actually work, will need some investigation
              RowParserError.classCast(i).left
          }
        }
    }

  def getAsOption[A]: RowParser[Option[A]] =
    getAs[A].map(Option(_))
}

sealed trait RowParserError {

  def render: String =
    this match {
      case RowParserError.ClassCast(i) =>
        s"Invalid column type parsed at column $i"
      case RowParserError.IndexOutOfBounds(i) =>
        s"Invalid column index for column $i"
      case RowParserError.InvalidValue(i, message) =>
        s"Invalid value at column $i - $message"
    }
}

object RowParserError {

  case class ClassCast(i: Int) extends RowParserError
  case class IndexOutOfBounds(i: Int) extends RowParserError
  case class InvalidValue(i: Int, message: String) extends RowParserError

  def classCast(i: Int): RowParserError =
    ClassCast(i)

  def indexOutOfBounds(i: Int): RowParserError =
    IndexOutOfBounds(i)

  def invalidValue(i: Int, message: String): RowParserError =
    InvalidValue(i, message)
}
