package redis.protocol

import akka.util.ByteString
import redis.TestBase

class RedisProtocolRequestSpec extends TestBase {

  "Encode request" should {
    "inline" in {
      RedisProtocolRequest.inline("PING") shouldBe ByteString("PING\r\n")
    }
    "multibulk" in {
      val encoded = RedisProtocolRequest.multiBulk("SET", Seq(ByteString("mykey"), ByteString("myvalue")))
      encoded shouldBe ByteString("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n")
    }
  }

}
