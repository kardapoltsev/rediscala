package redis

import akka.util.ByteString
import redis.protocol.{Bulk, RedisReply}

case class DumbClass(s1: String, s2: String)

object DumbClass {
  implicit val byteStringFormatter = new ByteStringFormatter[DumbClass] {
    def serialize(data: DumbClass): ByteString = {
      ByteString(data.s1 + "|" + data.s2)
    }

    def deserialize(bs: ByteString): DumbClass = {
      val r = bs.utf8String.split('|').toList
      DumbClass(r(0), r(1))
    }
  }

  implicit val redisReplyDeserializer =  new RedisReplyDeserializer[DumbClass] {
    override def deserialize: PartialFunction[RedisReply, DumbClass] = {
      case Bulk(Some(bs)) => byteStringFormatter.deserialize(bs)
    }
  }
}

class ConverterSpec extends TestBase {

  import redis.ByteStringSerializer._

  "ByteStringSerializer" should {
    "String" in {
      String.serialize("super string !") shouldBe ByteString("super string !")
    }

    "Short" in {
      ShortConverter.serialize(123) shouldBe ByteString("123")
    }

    "Int" in {
      IntConverter.serialize(123) shouldBe ByteString("123")
    }

    "Long" in {
      LongConverter.serialize(123) shouldBe ByteString("123")
    }

    "Float" in {
      FloatConverter.serialize(123.123f) shouldBe ByteString("123.123")
    }

    "Double" in {
      DoubleConverter.serialize(123.123456) shouldBe ByteString("123.123456")
    }

    "Char" in {
      CharConverter.serialize('a') shouldBe ByteString('a')
    }

    "Byte" in {
      ByteConverter.serialize(123) shouldBe ByteString(123)
    }

    "ArrayByte" in {
      ArrayByteConverter.serialize(Array[Byte](1, 2, 3)) shouldBe ByteString(Array[Byte](1, 2, 3))
    }

    "ByteString" in {
      ByteStringConverter.serialize(ByteString("stupid")) shouldBe ByteString("stupid")
    }
  }

  "ByteStringFormatter" should {
    "DumbClass" in {
      val dumb = DumbClass("aa", "bb")

      val formatter = implicitly[ByteStringFormatter[DumbClass]]

      formatter.serialize(dumb) shouldBe ByteString("aa|bb")
      formatter.deserialize(ByteString("aa|bb")) shouldBe dumb
    }
  }

}
