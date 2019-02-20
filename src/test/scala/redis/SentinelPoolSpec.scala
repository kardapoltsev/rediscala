package redis

import redis.RedisServerHelper.redisHost

class SentinelMutablePoolSpec extends RedisSentinelClients("SentinelMutablePoolSpec") {

  var redisPool: RedisClientMutablePool = null

  override def setup(): Unit = {
    super.setup()
    redisPool = RedisClientMutablePool(Seq(RedisServer(redisHost, slavePort1)), masterName)
  }

  "mutable pool" should {
    "add remove" in {
      eventually {
        redisPool.redisConnectionPool.size shouldBe 1
      }

      redisPool.addServer(RedisServer(redisHost, slavePort2))
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

class SentinelMonitoredRedisClientMasterSlavesSpec
    extends RedisSentinelClients("SentinelMonitoredRedisClientMasterSlavesSpec") {

  lazy val redisMasterSlavesPool =
    SentinelMonitoredRedisClientMasterSlaves(master = masterName, sentinels = sentinelPorts.map((redisHost, _)))
  "sentinel slave pool" should {
    "add and remove" in {
      eventually {
        redisMasterSlavesPool.set("test", "value").futureValue
        redisMasterSlavesPool.slavesClients.redisConnectionPool.size shouldBe 2
      }

      val newSlave = newSlaveProcess()

      eventually {
        redisMasterSlavesPool.slavesClients.redisConnectionPool.size shouldBe 3
      }
      newSlave.stop()

      eventually {
        redisMasterSlavesPool.get[String]("test").futureValue shouldBe Some("value")
      }
      slave1.stop()
      slave2.stop()

      eventually {
        redisMasterSlavesPool.slavesClients.redisConnectionPool.size shouldBe 0
      }

      redisMasterSlavesPool.get[String]("test").futureValue shouldBe Some("value")
      newSlaveProcess()

      eventually {
        redisMasterSlavesPool.slavesClients.redisConnectionPool.size shouldBe 1
      }
    }
    /*
   "changemaster" in {
     Try(redisMasterSlavesPool.masterClient.shutdown(), timeOut))
       awaitAssert( redisMasterSlavesPool.slavesClients.redisConnectionPool.size shouldBe 0, 20 second )
       redisMasterSlavesPool.get[String]("test"), timeOut) shouldBe Some("value")
   }*/

  }
}
