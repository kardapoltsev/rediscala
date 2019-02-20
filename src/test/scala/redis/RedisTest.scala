package redis

import akka.ConfigurationException

import scala.concurrent._
import akka.util.ByteString

class RedisTest extends RedisStandaloneServer {

  "basic test" should {
    "ping" in {
      Await.result(redis.ping, timeOut) shouldBe "PONG"
    }
    "set" in {
      Await.result(redis.set("key", "value"), timeOut) shouldBe true
    }
    "get" in {
      Await.result(redis.get("key"), timeOut) shouldBe Some(ByteString("value"))
    }
    "del" in {
      Await.result(redis.del("key"), timeOut) shouldBe 1
    }
    "get not found" in {
      Await.result(redis.get("key"), timeOut) shouldBe None
    }
  }

  "init connection test" should {
    "ok" in {
      withRedisServer(port => {
        val redis = RedisClient(port = port)
        // TODO set password (CONFIG SET requiredpass password)
        val r = for {
          _ <- redis.select(2)
          _ <- redis.set("keyDbSelect", "2")
        } yield {
          val redis = RedisClient(port = port, password = Some("password"), db = Some(2))
          Await.result(redis.get[String]("keyDbSelect"), timeOut) shouldBe Some("2")
        }
        Await.result(r, timeOut)
      })
    }
    "use custom dispatcher" in {
      a[ConfigurationException] shouldBe thrownBy {
        withRedisServer(port => {
          implicit val redisDispatcher = RedisDispatcher("no-this-dispatcher")
          RedisClient(port = port)
        })
      }
    }
  }

}
