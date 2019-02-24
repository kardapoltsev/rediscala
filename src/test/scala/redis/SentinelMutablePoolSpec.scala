package redis

import redis.RedisServerHelper.redisHost

class SentinelMutablePoolSpec extends RedisSentinelClients("SentinelMutablePoolSpec") {

  private val redisPool = RedisClientMutablePool(Seq(RedisServer(redisHost, slavePort1)), masterName)

  "mutable pool" should {
    "add remove" in {
      log.debug("checking initial pool size")
      eventually {
        redisPool.redisConnectionPool.size shouldBe 1
      }
      log.debug("adding new redis server")
      redisPool.addServer(RedisServer(redisHost, slavePort2))
      log.debug("checking new pool size")
      eventually {
        redisPool.redisConnectionPool.size shouldBe 2
      }
      redisPool.addServer(RedisServer(redisHost, slavePort2))
      eventually {
        redisPool.redisConnectionPool.size shouldBe 2
      }

      val key = "keyPoolDb0"
      redisClient.set(key, "hello").futureValue

      redisPool.get[String](key).futureValue shouldBe Some("hello")
      redisPool.get[String](key).futureValue shouldBe Some("hello")

      redisPool.removeServer(RedisServer(redisHost, slavePort2))

      eventually {
        redisPool.redisConnectionPool.size shouldBe 1
      }

      redisPool.get[String](key).futureValue shouldBe Some("hello")
      redisPool.get[String](key).futureValue shouldBe Some("hello")
    }
  }

}
