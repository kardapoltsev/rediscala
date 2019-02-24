package redis

import redis.RedisServerHelper.redisHost

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
