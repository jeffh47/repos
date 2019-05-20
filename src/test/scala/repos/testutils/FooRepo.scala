package repos.testutils

import java.util.UUID

import repos.{DataMapper, Repo, IdType}
import StringDataMapper.stringDataMapper

case class FooId(untyped: UUID) extends AnyVal with IdType

object FooRepo extends Repo[FooId, String]("foo") {
  def lengthIndex = indexTable("len_index")(_.length.toLong)

  def textIndex = indexTable("text_text")(s => s)

  def firstCharIndex = partialIndexTable("first_ch") {
    case t if t.size > 0 => t(0)
  }

  def firstTwoIndex = indexTable("first_two_ch")(_.take(2))

  def takeAtLeastTwo : PartialFunction[String,String] = {
    case s if s.size >= 2 => s.take(2)
  }

  def firstAtLeastTwoIndex = partialIndexTable("first_at_least_two_ch")(takeAtLeastTwo)

  def firstAtLeastTwoLatestIndex = partialIndexTable("first_at_least_two_ch_latest", true)(takeAtLeastTwo)

  def seqIndex = multiIndexTable("seq")(_.toSeq)
}

object StringDataMapper {
  implicit val stringDataMapper: DataMapper[String] = new DataMapper[String] {
    override def toBytes(m: String): Array[Byte] = m.getBytes()

    override def fromBytes(b: Array[Byte]): String = new String(b)
  }
}

