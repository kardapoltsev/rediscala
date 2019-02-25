package redis

import redis.api.connection.Select

import scala.concurrent._
import scala.concurrent.duration._
class RedisPoolSpec extends RedisStandaloneServer {


  "basic pool test" should {
    "ok" in {
      val redisPool = RedisClientPool(Seq(RedisServer(  port = port,db = Some(0)), RedisServer(  port = port,db = Some(1)), RedisServer(  port = port,db = Some(3))))
      val key = "keyPoolDb0"
      redisPool.set(key, 0)
      val r = for {
        getDb1 <- redisPool.get(key)
        getDb2 <- redisPool.get(key)
        getDb0 <- redisPool.get[String](key)
        select <- Future.sequence(redisPool.broadcast(Select(0)))
        getKey1 <- redisPool.get[String](key)
        getKey2 <- redisPool.get[String](key)
        getKey0 <- redisPool.get[String](key)
      } yield {
        getDb1 shouldBe empty
        getDb2 shouldBe empty
        getDb0 shouldBe Some("0")
        select shouldBe Seq(true, true, true)
        getKey1 shouldBe Some("0")
        getKey2 shouldBe Some("0")
        getKey0 shouldBe Some("0")
      }
      r.futureValue
    }
    
    "check status" in {
      val redisPool = RedisClientPool(Seq(RedisServer(  port = port,db = Some(0)), RedisServer(  port = port,db = Some(1)), RedisServer(port = 3333,db = Some(3))))
      val key = "keyPoolDb0"

      eventually{
        redisPool.redisConnectionPool.size shouldBe 2
      }
      redisPool.set(key, 0)
      val r = for {
        getDb1 <- redisPool.get(key)
        getDb0 <- redisPool.get[String](key)
        select <- Future.sequence(redisPool.broadcast(Select(0)))
        getKey1 <- redisPool.get[String](key)
        getKey0 <- redisPool.get[String](key)
      } yield {
        getDb1 shouldBe empty
        getDb0 shouldBe Some("0")
        select shouldBe Seq(true, true)
        getKey1 shouldBe Some("0")
        getKey0 shouldBe Some("0")
      }
      r.futureValue

    }
  }
}
