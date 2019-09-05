package redis

class SentinelSpec extends RedisSentinelClients("SentinelSpec") {

  "sentinel monitored test" should {

    "master auto failover" in {
      val port = sentinelMonitoredRedisClient.redisClient.port

      sentinelMonitoredRedisClient.ping().futureValue shouldBe "PONG"
      sentinelClient.failover(masterName).futureValue shouldBe true

      sentinelMonitoredRedisClient.ping().futureValue shouldBe "PONG"
      Seq(slavePort1, slavePort2, port) should contain(sentinelMonitoredRedisClient.redisClient.port)

      sentinelMonitoredRedisClient.ping().futureValue shouldBe "PONG"
      Seq(slavePort1, slavePort2, masterPort, port) should contain(sentinelMonitoredRedisClient.redisClient.port)
    }

    "ping" in {
      sentinelMonitoredRedisClient.ping().futureValue shouldBe "PONG"
      redisClient.ping().futureValue shouldBe "PONG"
    }

    "sentinel nodes auto discovery" in {
      val sentinelCount = sentinelMonitoredRedisClient.sentinelClients.size
      val sentinel = newSentinelProcess()

      eventually {
        sentinelMonitoredRedisClient.sentinelClients.size shouldBe sentinelCount + 1
      }
      sentinel.stop()
      eventually {
        sentinelMonitoredRedisClient.sentinelClients.size shouldBe sentinelCount
      }
    }
  }

  "sentinel test" should {
    "masters" in {
      val r = sentinelClient.masters().futureValue
      r(0)("name") shouldBe masterName
      r(0)("flags").startsWith("master") shouldBe true
    }
    "no such master" in {
      val opt = sentinelClient.getMasterAddr("no-such-master").futureValue
      withClue(s"unexpected: master with name '$masterName' was not supposed to be found") {
        opt shouldBe empty
      }
    }
    "unknown master state" in {
      val opt = sentinelClient.isMasterDown("no-such-master").futureValue
      withClue("unexpected: master state should be unknown") { opt shouldBe empty }
    }
    "master ok" in {
      withClue(s"unexpected: master with name '$masterName' was not found") {
        sentinelClient.isMasterDown(masterName).futureValue shouldBe Some(false)
      }
    }
    "slaves" in {
      val r = sentinelClient.slaves(masterName).futureValue
      r should not be empty
      r(0)("flags").startsWith("slave") shouldBe true
    }
    "reset bogus master" in {
      sentinelClient.resetMaster("no-such-master").futureValue shouldBe false
    }
    "reset master" in {
      sentinelClient.resetMaster(masterName).futureValue shouldBe true
    }
  }

}
