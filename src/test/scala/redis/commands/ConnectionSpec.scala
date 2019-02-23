package redis.commands

import redis._
import akka.util.ByteString
import redis.actors.ReplyErrorException

class ConnectionSpec extends RedisStandaloneServer {

  "Connection commands" should {
    "AUTH" in {
      redis.auth("no password").failed.futureValue shouldBe a[ReplyErrorException]
    }
    "ECHO" in {
      val hello = "Hello World!"
      redis.echo(hello).futureValue shouldBe Some(ByteString(hello))
    }
    "PING" in {
      redis.ping().futureValue shouldBe "PONG"
    }
    "QUIT" in {
      // todo test that the TCP connection is reset.
      redis.quit().futureValue shouldBe true
    }
    "SELECT" in {
      redis.select(0).futureValue shouldBe true
      redis.select(-1).failed.futureValue shouldBe a[ReplyErrorException]
    }
  }
}
