package redis.commands

import akka.util.ByteString
import redis._
import redis.actors.ReplyErrorException

import scala.concurrent.{Await, Future}

class StringsSpec extends RedisStandaloneServer {

  "Strings commands" should {
    "APPEND" in {
      val r = redis
        .set("appendKey", "Hello")
        .flatMap(_ => {
          redis
            .append("appendKey", " World")
            .flatMap(length => {
              length shouldBe "Hello World".length
              redis.get("appendKey")
            })
        })
      Await.result(r, timeOut) shouldBe Some(ByteString("Hello World"))
    }

    "BITCOUNT" in {
      val r = redis
        .set("bitcountKey", "foobar")
        .flatMap(_ => {
          val a = redis.bitcount("bitcountKey")
          val b = redis.bitcount("bitcountKey", 0, 0)
          val c = redis.bitcount("bitcountKey", 1, 1)
          Future.sequence(Seq(a, b, c))
        })
      Await.result(r, timeOut) shouldBe Seq(26, 4, 6)
    }

    "BITOP" in {
      val s1 = redis.set("bitopKey1", "afoobar a")
      val s2 = redis.set("bitopKey2", "aabcdef a")
      val r = for {
        _   <- s1
        _   <- s2
        and <- redis.bitopAND("ANDbitopKey", "bitopKey1", "bitopKey2")
        or  <- redis.bitopOR("ORbitopKey", "bitopKey1", "bitopKey2")
        xor <- redis.bitopXOR("XORbitopKey", "bitopKey1", "bitopKey2")
        not <- redis.bitopNOT("NOTbitopKey", "bitopKey1")
      } yield {
        withClue("AND") {
          Await.result(redis.get("ANDbitopKey"), timeOut) shouldBe Some(ByteString("a`bc`ab a"))
        }
        withClue("OR") {
          Await.result(redis.get("ORbitopKey"), timeOut) shouldBe Some(ByteString("agoofev a"))
        }
        withClue("XOR") {
          Await.result(redis.get("XORbitopKey"), timeOut) shouldBe Some(ByteString(0, 7, 13, 12, 6, 4, 20, 0, 0))
        }
        withClue("NOT") {
          Await.result(redis.get("NOTbitopKey"), timeOut) shouldBe Some(
            ByteString(-98, -103, -112, -112, -99, -98, -115, -33, -98))
        }
      }
      Await.result(r, timeOut)
    }

    "BITPOS" in {
      val r = for {
        s1 <- redis.set("bitposKey", "a+b") // 01100001 00101011 01100010
        v1 <- redis.bitpos("bitposKey", 0)
        v2 <- redis.bitpos("bitposKey", 1)
        v3 <- redis.bitpos("bitposKey", 1, 1)
        v4 <- redis.bitpos("bitposKey", 0, 3)
        v5 <- redis.bitpos("bitposKey", 0, 1, 2)
      } yield {
        s1 shouldBe true
        v1 shouldBe 0
        v2 shouldBe 1
        v3 shouldBe 10
        v4 shouldBe -1
        v5 shouldBe 8
      }
      Await.result(r, timeOut)
    }

    "DECR" in {
      val r = redis
        .set("decrKey", "10")
        .flatMap(_ => {
          redis.decr("decrKey")
        })
      val r2 = redis
        .set("decrKeyError", "234293482390480948029348230948")
        .flatMap(_ => {
          redis.decr("decrKeyError")
        })
      Await.result(r, timeOut) shouldBe 9
      an[ReplyErrorException] should be thrownBy Await.result(r2, timeOut)
    }

    "DECRBY" in {
      val r = redis
        .set("decrbyKey", "10")
        .flatMap(_ => {
          redis.decrby("decrbyKey", 5)
        })
      Await.result(r, timeOut) shouldBe 5
    }

    "GET" in {
      val r = redis.get("getKeyNonexisting")
      val r2 = redis
        .set("getKey", "Hello")
        .flatMap(_ => {
          redis.get("getKey")
        })
      Await.result(r, timeOut) shouldBe None
      Await.result(r2, timeOut) shouldBe Some(ByteString("Hello"))

      val rrr = for {
        r3 <- redis.get[String]("getKey")
      } yield {
        r3 shouldBe Some("Hello")
      }
      Await.result(rrr, timeOut)
    }

    "GET with conversion" in {
      val dumbObject = new DumbClass("foo", "bar")
      val r = redis
        .set("getDumbKey", dumbObject)
        .flatMap(_ => {
          redis.get[DumbClass]("getDumbKey")
        })
      Await.result(r, timeOut) shouldBe Some(dumbObject)
    }

    "GETBIT" in {
      val r = redis.getbit("getbitKeyNonexisting", 0)
      val r2 = redis
        .set("getbitKey", "Hello")
        .flatMap(_ => {
          redis.getbit("getbitKey", 1)
        })
      Await.result(r, timeOut) shouldBe false
      Await.result(r2, timeOut) shouldBe true
    }

    "GETRANGE" in {
      val r = redis
        .set("getrangeKey", "This is a string")
        .flatMap(_ => {
          Future.sequence(
            Seq(
              redis.getrange("getrangeKey", 0, 3),
              redis.getrange("getrangeKey", -3, -1),
              redis.getrange("getrangeKey", 0, -1),
              redis.getrange("getrangeKey", 10, 100)
            ).map(_.map(_.map(_.utf8String).get)))
        })
      Await.result(r, timeOut) shouldBe Seq("This", "ing", "This is a string", "string")
    }

    "GETSET" in {
      val r = redis
        .set("getsetKey", "Hello")
        .flatMap(_ => {
          redis
            .getset("getsetKey", "World")
            .flatMap(hello => {
              hello shouldBe Some(ByteString("Hello"))
              redis.get("getsetKey")
            })
        })
      Await.result(r, timeOut) shouldBe Some(ByteString("World"))
    }

    "INCR" in {
      val r = redis
        .set("incrKey", "10")
        .flatMap(_ => {
          redis.incr("incrKey")
        })
      Await.result(r, timeOut) shouldBe 11
    }

    "INCRBY" in {
      val r = redis
        .set("incrbyKey", "10")
        .flatMap(_ => {
          redis.incrby("incrbyKey", 5)
        })
      Await.result(r, timeOut) shouldBe 15
    }

    "INCRBYFLOAT" in {
      val r = redis
        .set("incrbyfloatKey", "10.50")
        .flatMap(_ => {
          redis.incrbyfloat("incrbyfloatKey", 0.15)
        })
      Await.result(r, timeOut) shouldBe Some(10.65)
    }

    "MGET" in {
      val s1 = redis.set("mgetKey", "Hello")
      val s2 = redis.set("mgetKey2", "World")
      val r = for {
        _    <- s1
        _    <- s2
        mget <- redis.mget("mgetKey", "mgetKey2", "mgetKeyNonexisting")
      } yield {
        mget shouldBe Seq(Some(ByteString("Hello")), Some(ByteString("World")), None)
      }
      Await.result(r, timeOut)
    }

    "MSET" in {
      val r = redis
        .mset(Map("msetKey" -> "Hello", "msetKey2" -> "World"))
        .flatMap(ok => {
          ok shouldBe true
          Future.sequence(
            Seq(
              redis.get("msetKey"),
              redis.get("msetKey2")
            ))
        })
      Await.result(r, timeOut) shouldBe Seq(Some(ByteString("Hello")), Some(ByteString("World")))
    }

    "MSETNX" in {
      val r = for {
        _           <- redis.del("msetnxKey", "msetnxKey2")
        msetnx      <- redis.msetnx(Map("msetnxKey" -> "Hello", "msetnxKey2" -> "World"))
        msetnxFalse <- redis.msetnx(Map("msetnxKey3" -> "Hello", "msetnxKey2" -> "already set !!"))
      } yield {
        msetnx shouldBe true
        msetnxFalse shouldBe false
      }
      Await.result(r, timeOut)
    }

    "PSETEX" in {
      redis.psetex("psetexKey", 1000, "temp value").futureValue shouldBe true
      redis.get("psetexKey").futureValue shouldBe Some(ByteString("temp value"))
      eventually { redis.get("psetexKey").futureValue shouldBe empty }
    }

    "SET" in {
      redis.set("setKey", "value").futureValue shouldBe true
      redis.set("setKey", "value", exSeconds = Some(2)).futureValue shouldBe true
      redis.set("setKey2", "value", NX = true, exSeconds = Some(60)).futureValue shouldBe true
      redis.ttl("setKey2").futureValue should beBetween(0L, 60L)
      redis.set("setKey2", "value", XX = true, exSeconds = Some(180)).futureValue shouldBe true
      redis.ttl("setKey2").futureValue should beBetween(60L, 180L)
      redis.del("setKey2").futureValue
      redis.set("setKey", "value", pxMilliseconds = Some(1)).futureValue shouldBe true
      eventually {
        redis.set("setKey", "value", NX = true).futureValue shouldBe true
      }
      redis.set("setKey", "value", XX = true).futureValue shouldBe true
      redis.set("setKey", "value", NX = true).futureValue shouldBe false
    }

    "SETBIT" in {
      val r = for {
        _        <- redis.del("setbitKey")
        setTrue  <- redis.setbit("setbitKey", 1, value = true)
        getTrue  <- redis.getbit("setbitKey", 1)
        setFalse <- redis.setbit("setbitKey", 1, value = false)
        getFalse <- redis.getbit("setbitKey", 1)
      } yield {
        setTrue shouldBe false
        getTrue shouldBe true
        setFalse shouldBe true
        getFalse shouldBe false
      }
      Await.result(r, timeOut)
    }

    "SETEX" in {
      redis.setex("setexKey", 1, "temp value").futureValue shouldBe true
      redis.get("setexKey").futureValue shouldBe Some(ByteString("temp value"))
      eventually { redis.get("setexKey").futureValue shouldBe empty }
    }

    "SETNX" in {
      val r = for {
        _  <- redis.del("setnxKey")
        s1 <- redis.setnx("setnxKey", "Hello")
        s2 <- redis.setnx("setnxKey", "World")
      } yield {
        s1 shouldBe true
        s2 shouldBe false
      }
      Await.result(r, timeOut)
    }

    "SETRANGE" in {
      val r = redis
        .set("setrangeKey", "Hello World")
        .flatMap(d => {
          redis
            .setrange("setrangeKey", 6, "Redis")
            .flatMap(length => {
              length shouldBe "Hello Redis".length
              redis.get("setrangeKey")
            })
        })
      Await.result(r, timeOut) shouldBe Some(ByteString("Hello Redis"))
    }

    "STRLEN" in {
      val r = redis
        .set("strlenKey", "Hello World")
        .flatMap(d => {
          redis
            .strlen("strlenKey")
            .flatMap(length => {
              length shouldBe "Hello World".length
              redis.strlen("strlenKeyNonexisting")
            })
        })
      Await.result(r, timeOut) shouldBe 0
    }
  }
}
