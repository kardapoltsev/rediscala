package redis.issues

import redis._

class Issue26Spec extends RedisStandaloneServer {

  "Deserializer exceptions" should {
    "be propagated to resulting future" in {
      redis.set("key", "value").futureValue shouldBe true
      assertThrows[NumberFormatException](redis.get[Double]("key").futureValue)
    }
  }
}
