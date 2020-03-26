package redis.issues

import org.scalatest.RecoverMethods._
import org.scalatest.Succeeded
import redis._

class Issue26Spec extends RedisStandaloneServer {

  "Deserializer exceptions" should {
    "be propagated to resulting future" in {
      redis.set("key", "value").futureValue shouldBe true
      recoverToSucceededIf[NumberFormatException](redis.get[Double]("key")).futureValue shouldBe Succeeded
    }
  }
}
