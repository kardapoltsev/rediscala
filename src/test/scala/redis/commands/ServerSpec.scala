package redis.commands

import redis._
import scala.concurrent.Await
import redis.actors.{InvalidRedisReply, ReplyErrorException}
import redis.api.NOSAVE

class ServerSpec extends RedisStandaloneServer {

  "Server commands" should {

    "BGSAVE" in {
      Await.result(redis.bgsave(), timeOut) shouldBe "Background saving started"
    }

    "CLIENT KILL" in {
      the[ReplyErrorException] thrownBy {
        Await.result(redis.clientKill("8.8.8.8", 53), timeOut)
      }
    }

    "CLIENT LIST" in {
      val list = Await.result(redis.clientList(), timeOut)
      list shouldBe a[Seq[Map[String, String]]]
      list should not be empty
    }

    "CLIENT GETNAME" in {
      Await.result(redis.clientGetname(), timeOut) shouldBe None
    }

    "CLIENT SETNAME" in {
      Await.result(redis.clientSetname("rediscala"), timeOut) shouldBe true
    }

    "CONFIG GET" in {
      val map = Await.result(redis.configGet("*"), timeOut)
      map shouldBe a[Map[String, String]]
      map should not be empty

    }
    "CONFIG SET" in {
      val r = for {
        set      <- redis.configSet("loglevel", "warning")
        loglevel <- redis.configGet("loglevel")
      } yield {
        set shouldBe true
        loglevel.get("loglevel") shouldBe Some("warning")
      }
      Await.result(r, timeOut)
    }

    "CONFIG RESETSTAT" in {
      Await.result(redis.configResetstat(), timeOut) shouldBe true
    }

    "DBSIZE" in {
      Await.result(redis.dbsize(), timeOut) should be >= 0L
    }

    "DEBUG OBJECT" in {
      redis.debugObject("serverDebugObj").failed.futureValue shouldBe a[ReplyErrorException]
    }

    "DEBUG SEGFAULT" ignore {}

    "FLUSHALL" in {
      Await.result(redis.flushall(), timeOut) shouldBe true
    }

    "FLUSHDB" in {
      Await.result(redis.flushdb(), timeOut) shouldBe true
    }

    "INFO" in {
      val r = for {
        info    <- redis.info()
        infoCpu <- redis.info("cpu")
      } yield {
        info shouldBe a[String]
        infoCpu shouldBe a[String]
      }
      Await.result(r, timeOut)
    }

    "LASTSAVE" in {
      Await.result(redis.lastsave(), timeOut) should be >= 0L
    }

    "SAVE" in {
      val result = try { Await.result(redis.save(), timeOut) } catch {
        case ReplyErrorException("ERR Background save already in progress") => true
      }
      result shouldBe true
    }

    "SLAVE OF" in {
      Await.result(redis.slaveof("server", 12345), timeOut) shouldBe true
    }

    "SLAVE OF NO ONE" in {
      Await.result(redis.slaveofNoOne(), timeOut) shouldBe true
    }

    "TIME" in {
      val result = Await.result(redis.time(), timeOut)
      withClue(result.toString()) {

        result shouldBe a[Tuple2[Long, Long]]
      }
    }

    "BGREWRITEAOF" in {
      // depending on the redis version, this string could vary, redis 2.8.21 says 'scheduled'
      // but redis 2.8.18 says 'started'
      val r = Await.result(redis.bgrewriteaof(), timeOut)
      r should (be("Background append only file rewriting started") or
        be("Background append only file rewriting scheduled"))
    }

    "SHUTDOWN" in {
      a[InvalidRedisReply.type] should be thrownBy Await.result(redis.shutdown(), timeOut)
    }

    "SHUTDOWN (with modifier)" in {
      withRedisServer(port => {
        val redis = RedisClient(port = port)
        a[InvalidRedisReply.type] should be thrownBy Await.result(redis.shutdown(NOSAVE), timeOut)
      })
    }

  }
}
