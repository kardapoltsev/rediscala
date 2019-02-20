package redis.commands

import redis._
import scala.concurrent.Await
import akka.util.ByteString
import redis.actors.ReplyErrorException

class ConnectionSpec extends RedisStandaloneServer {

  "Connection commands" should {
    "AUTH" in {
      a[ReplyErrorException] should be thrownBy Await.result(redis.auth("no password"), timeOut)
    }
    "ECHO" in {
      val hello = "Hello World!"
      Await.result(redis.echo(hello), timeOut) shouldBe Some(ByteString(hello))
    }
    "PING" in {
      Await.result(redis.ping(), timeOut) shouldBe "PONG"
    }
    "QUIT" in {
      // todo test that the TCP connection is reset.
      val f = redis.quit()
      Thread.sleep(1000)
      val ping = redis.ping()
      Await.result(f, timeOut) shouldBe true
      Await.result(ping, timeOut) shouldBe "PONG"
    }
    "SELECT" in {
      Await.result(redis.select(1), timeOut) shouldBe true
      Await.result(redis.select(0), timeOut) shouldBe true
      a[ReplyErrorException] should be thrownBy Await.result(redis.select(-1), timeOut)
      a[ReplyErrorException] should be thrownBy Await.result(redis.select(1000), timeOut)
    }
  }
}
