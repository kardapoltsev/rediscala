package redis

import redis.RedisServerHelper.redisHost

import scala.concurrent.duration._
class SentinelMutablePoolSpec extends RedisSentinelClients("SentinelMutablePoolSpec") {

  var redisPool: RedisClientMutablePool = null

  override def setup(): Unit = {
    super.setup()
    redisPool = RedisClientMutablePool(Seq(RedisServer(redisHost, slavePort1)), masterName)
  }

  "mutable pool" should {
    "add remove" in {
      Thread.sleep(1000)
      redisPool.redisConnectionPool.size shouldBe 1

      redisPool.addServer(RedisServer(redisHost, slavePort2))
      redisPool.addServer(RedisServer(redisHost, slavePort2))
      Thread.sleep(5000)
      redisPool.redisConnectionPool.size shouldBe 2

      val key = "keyPoolDb0"
      redisClient.set(key, "hello").futureValue

      redisPool.get[String](key).futureValue shouldBe Some("hello")
      redisPool.get[String](key).futureValue shouldBe Some("hello")

      within(1 second) {
        redisPool.removeServer(RedisServer(redisHost, slavePort2))
      }

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
      Thread.sleep(10000)
      redisMasterSlavesPool.set("test", "value").futureValue
      eventually {
        redisMasterSlavesPool.slavesClients.redisConnectionPool.size shouldBe 2
      }

      val newSlave = newSlaveProcess()

      eventually {
        redisMasterSlavesPool.slavesClients.redisConnectionPool.size shouldBe 3
      }
      newSlave.stop()

      redisMasterSlavesPool.get[String]("test").futureValue shouldBe Some("value")
      slave1.stop()
      slave2.stop()

      awaitAssert(redisMasterSlavesPool.slavesClients.redisConnectionPool.size shouldBe 0, 20 second)
      redisMasterSlavesPool.get[String]("test").futureValue shouldBe Some("value")
      newSlaveProcess()
      //println("************************** newSlaveProcess "+RedisServerHelper.portNumber.get())

      eventually {
        redisMasterSlavesPool.slavesClients.redisConnectionPool.size shouldBe 1
      }
      redisMasterSlavesPool.slavesClients.redisConnectionPool.size shouldBe 1
    }
    /*
   "changemaster" in {
     Try(redisMasterSlavesPool.masterClient.shutdown(), timeOut))
       awaitAssert( redisMasterSlavesPool.slavesClients.redisConnectionPool.size shouldBe 0, 20 second )
       redisMasterSlavesPool.get[String]("test"), timeOut) shouldBe Some("value")
   }*/

  }
}
