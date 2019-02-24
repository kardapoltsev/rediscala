package redis

import akka.ConfigurationException

import akka.util.ByteString

class RedisTest extends RedisStandaloneServer {

  "basic test" should {
    "ping" in {
      redis.ping.futureValue shouldBe "PONG"
    }
    "set" in {
      redis.set("key", "value").futureValue shouldBe true
    }
    "get" in {
      redis.get("key").futureValue shouldBe Some(ByteString("value"))
    }
    "del" in {
      redis.del("key").futureValue shouldBe 1
    }
    "get not found" in {
      redis.get("key").futureValue shouldBe None
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
          redis.get[String]("keyDbSelect").futureValue shouldBe Some("2")
        }
        r.futureValue
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
