package redis.commands

import redis._
import scala.concurrent.Await
import akka.util.ByteString
import scala.concurrent.duration._

class BListsSpec extends RedisStandaloneServer {

  "Blocking Lists commands" should {

    "BLPOP already containing elements" in {
      val redisB = RedisBlockingClient(port = port)
      val r = for {
        _ <- redis.del("blpop1", "blpop2")
        p <- redis.rpush("blpop1", "a", "b", "c")
        b <- redisB.blpop(Seq("blpop1", "blpop2"))
      } yield {
        b shouldBe Some("blpop1" -> ByteString("a"))
      }
      val rr = Await.result(r, timeOut)
      redisB.stop()
      rr
    }

    "BLPOP blocking" in {
      val redisB = RedisBlockingClient(port = port)
      val blockTime = 1.second
      val rr = within(blockTime, blockTime * 2) {
        val r = redis
          .del("blpopBlock")
          .flatMap(_ => {
            val blpop = redisB.blpop(Seq("blpopBlock"))
            Thread.sleep(blockTime.toMillis)
            redis.rpush("blpopBlock", "a", "b", "c")
            blpop
          })
        Await.result(r, timeOut) shouldBe Some("blpopBlock" -> ByteString("a"))
      }
      redisB.stop()
      rr
    }

    "BLPOP blocking timeout" in {
      val redisB = RedisBlockingClient(port = port)
      val rr = within(1.seconds, 10.seconds) {
        val r = redis
          .del("blpopBlockTimeout")
          .flatMap(_ => {
            redisB.brpop(Seq("blpopBlockTimeout"), 1.seconds)
          })
        Await.result(r, timeOut) shouldBe empty
      }
      redisB.stop()
      rr
    }

    "BRPOP already containing elements" in {
      val redisB = RedisBlockingClient(port = port)
      val r = for {
        _ <- redis.del("brpop1", "brpop2")
        p <- redis.rpush("brpop1", "a", "b", "c")
        b <- redisB.brpop(Seq("brpop1", "brpop2"))
      } yield {
        redisB.stop()
        b shouldBe Some("brpop1" -> ByteString("c"))
      }
      Await.result(r, timeOut)
    }

    "BRPOP blocking" in {
      val redisB = RedisBlockingClient(port = port)
      val rr = within(1.seconds, 10.seconds) {
        val r = redis
          .del("brpopBlock")
          .flatMap(_ => {
            val brpop = redisB.brpop(Seq("brpopBlock"))
            Thread.sleep(1000)
            redis.rpush("brpopBlock", "a", "b", "c")
            brpop
          })
        Await.result(r, timeOut) shouldBe Some("brpopBlock" -> ByteString("c"))
      }
      redisB.stop()
      rr
    }

    "BRPOP blocking timeout" in {
      val redisB = RedisBlockingClient(port = port)
      val rr = within(1.seconds, 10.seconds) {
        val r = redis
          .del("brpopBlockTimeout")
          .flatMap(_ => {
            redisB.brpop(Seq("brpopBlockTimeout"), 1.seconds)
          })
        Await.result(r, timeOut) shouldBe empty
      }
      redisB.stop()
      rr
    }

    "BRPOPLPUSH already containing elements" in {
      val redisB = RedisBlockingClient(port = port)
      val r = for {
        _ <- redis.del("brpopplush1", "brpopplush2")
        p <- redis.rpush("brpopplush1", "a", "b", "c")
        b <- redisB.brpoplpush("brpopplush1", "brpopplush2")
      } yield {
        b shouldBe Some(ByteString("c"))
      }
      val rr = Await.result(r, timeOut)
      redisB.stop()
      rr
    }

    "BRPOPLPUSH blocking" in {
      val redisB = RedisBlockingClient(port = port)
      val rr = within(1.seconds, 10.seconds) {
        val r = redis
          .del("brpopplushBlock1", "brpopplushBlock2")
          .flatMap(_ => {
            val brpopplush = redisB.brpoplpush("brpopplushBlock1", "brpopplushBlock2")
            Thread.sleep(1000)
            redis.rpush("brpopplushBlock1", "a", "b", "c")
            brpopplush
          })
        Await.result(r, timeOut) shouldBe Some(ByteString("c"))
      }
      redisB.stop()
      rr
    }

    "BRPOPLPUSH blocking timeout" in {
      val redisB = RedisBlockingClient(port = port)
      val rr = within(1.seconds, 10.seconds) {
        val r = redis
          .del("brpopplushBlockTimeout1", "brpopplushBlockTimeout2")
          .flatMap(_ => {
            redisB.brpoplpush("brpopplushBlockTimeout1", "brpopplushBlockTimeout2", 1.seconds)
          })
        Await.result(r, timeOut) shouldBe empty
      }
      redisB.stop()
      rr
    }

  }
}
