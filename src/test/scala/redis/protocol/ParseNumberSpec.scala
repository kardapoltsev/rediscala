package redis.protocol

import akka.util.ByteString
import redis.TestBase

class ParseNumberSpec extends TestBase {


  "ParseNumber.parseInt" should {
    "ok" in {
      ParseNumber.parseInt(ByteString("0")) shouldBe 0
      ParseNumber.parseInt(ByteString("10")) shouldBe 10
      ParseNumber.parseInt(ByteString("-10")) shouldBe -10
      ParseNumber.parseInt(ByteString("-123456")) shouldBe -123456
      ParseNumber.parseInt(ByteString("1234567890")) shouldBe 1234567890
      ParseNumber.parseInt(ByteString("-1234567890")) shouldBe -1234567890
    }

    "null" in {
      a[NumberFormatException] should be thrownBy ParseNumber.parseInt(null)
    }

    "lone \"+\" or \"-\"" in {
      a[NumberFormatException] should be thrownBy ParseNumber.parseInt(ByteString("+"))
      a[NumberFormatException] should be thrownBy ParseNumber.parseInt(ByteString("-"))
    }

    "invalid first char" in {
      a[NumberFormatException] should be thrownBy ParseNumber.parseInt(ByteString("$"))
    }

    "empty" in {
      a[NumberFormatException] should be thrownBy ParseNumber.parseInt(ByteString.empty)
    }

    "invalid char" in {
      a[NumberFormatException] should be thrownBy ParseNumber.parseInt(ByteString("?"))
    }

    "limit min" in {
      val l1 : Long = java.lang.Integer.MIN_VALUE
      val l = l1 - 1
      a[NumberFormatException] should be thrownBy ParseNumber.parseInt(ByteString(l.toString))
    }

    "limit max" in {
      val l1 : Long = java.lang.Integer.MAX_VALUE
      val l = l1 + 1
      a[NumberFormatException] should be thrownBy ParseNumber.parseInt(ByteString(l.toString))
    }

    "not a number" in {
      a[NumberFormatException] should be thrownBy ParseNumber.parseInt(ByteString("not a number"))
    }

    "launch exception before integer overflow" in {
      a[NumberFormatException] should be thrownBy ParseNumber.parseInt(ByteString("-2147483650"))
    }

  }
}
