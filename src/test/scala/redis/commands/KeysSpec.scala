package redis.commands

import akka.util.ByteString
import redis._
import redis.api._

import scala.concurrent.{Await, Future}

import java.io.File

class KeysSpec extends RedisStandaloneServer {

  "Keys commands" should {
    "DEL" in {
      val r = for {
        s <- redis.set("delKey", "value")
        d <- redis.del("delKey", "delKeyNonexisting")
      } yield {
        s shouldBe true
        d shouldBe 1
      }
      Await.result(r, timeOut)
    }
    "DUMP" in {
      val k = "dumpKey"
      val v = "value"
      killDumpIfExists
      val r = for {
        s <- redis.set(key = k, value = v)
        d <- redis.dump(k)
        _ <- redis.del(k)
        rs <- redis.restore(k, serializedValue = d.get)
        value <- redis.get[String](k)
      } yield {
        s shouldBe true
        rs shouldBe true
      }
      Await.result(r, timeOut)
    }

    "EXISTS" in {
      val r = for {
        s <- redis.set("existsKey", "value")
        e <- redis.exists("existsKey")
        e2 <- redis.exists("existsKeyNonexisting")
      } yield {
        s shouldBe true
        e shouldBe true
        e2 shouldBe false
      }
      Await.result(r, timeOut)
    }

    "EXISTS variadic" in {
      val r = for {
        s <- redis.set("existsKey", "value")
        e <- redis.existsMany("existsKey", "existsKeyNonexisting")
        e2 <- redis.existsMany("existsKeyNonexisting")
      } yield {
        s shouldBe true
        e shouldBe 1
        e2 shouldBe 0
      }
      Await.result(r, timeOut)
    }

    "EXPIRE" in {
      redis.set("expireKey", "value").futureValue shouldBe true
      redis.expire("expireKey", 1).futureValue shouldBe true
      redis.expire("expireKeyNonexisting", 1).futureValue shouldBe false

      eventually {
        redis.get("expireKey").futureValue shouldBe empty
      }
    }

    "EXPIREAT" in {
      val r = for {
        s <- redis.set("expireatKey", "value")
        e <- redis.expireat("expireatKey", System.currentTimeMillis() / 1000)
        expired <- redis.get("expireatKey")
      } yield {
        s shouldBe true
        e shouldBe true
        expired shouldBe None
      }
      Await.result(r, timeOut)
    }

    "KEYS" in {
      val r = for {
        _ <- redis.set("keysKey", "value")
        _ <- redis.set("keysKey2", "value")
        k <- redis.keys("keysKey*")
        k2 <- redis.keys("keysKey?")
        k3 <- redis.keys("keysKeyNomatch")
      } yield {
        k should contain theSameElementsAs (Seq("keysKey2", "keysKey"))
        k2 should contain theSameElementsAs (Seq("keysKey2"))
        k3 shouldBe empty
      }
      Await.result(r, timeOut)
    }

    "MIGRATE" in {
      import scala.concurrent.duration._

      withRedisServer(port => {
        val redisMigrate = RedisClient("localhost", port)
        val key = "migrateKey-" + System.currentTimeMillis()
        val r = for {
          _ <- redis.set(key, "value")
          m <- redis.migrate("localhost", port, key, 0, 10 seconds)
          get <- redisMigrate.get(key)
        } yield {
          m shouldBe true
          get shouldBe Some(ByteString("value"))
        }
        Await.result(r, timeOut)
      })
    }

    "MOVE" in {
      val redisMove = RedisClient(port = port)
      val r = for {
        _ <- redis.set("moveKey", "value")
        _ <- redisMove.select(1)
        _ <- redisMove.del("moveKey")
        move <- redis.move("moveKey", 1)
        move2 <- redis.move("moveKey2", 1)
        get <- redisMove.get("moveKey")
        get2 <- redisMove.get("moveKey2")
      } yield {
        move shouldBe true
        move2 shouldBe false
        get shouldBe Some(ByteString("value"))
        get2 shouldBe None
      }
      Await.result(r, timeOut)
    }

    "REFCOUNT" in {
      val r = for {
        _ <- redis.set("objectRefcount", "objectRefcountValue")
        ref <- redis.objectRefcount("objectRefcount")
        refNotFound <- redis.objectRefcount("objectRefcountNotFound")
      } yield {
        ref shouldBe Some(1)
        refNotFound shouldBe empty
      }
      Await.result(r, timeOut)
    }
    "IDLETIME" in {
      val r = for {
        _ <- redis.set("objectIdletime", "objectRefcountValue")
        time <- redis.objectIdletime("objectIdletime")
        timeNotFound <- redis.objectIdletime("objectIdletimeNotFound")
      } yield {
        time shouldBe defined
        timeNotFound shouldBe empty
      }
      Await.result(r, timeOut)
    }
    "ENCODING" in {
      val r = for {
        _ <- redis.set("objectEncoding", "objectRefcountValue")
        encoding <- redis.objectEncoding("objectEncoding")
        encodingNotFound <- redis.objectEncoding("objectEncodingNotFound")
      } yield {
        encoding shouldBe defined
        encodingNotFound shouldBe empty
      }
      Await.result(r, timeOut)
    }

    "PERSIST" in {
      val r = for {
        s <- redis.set("persistKey", "value")
        e <- redis.expire("persistKey", 10)
        ttl <- redis.ttl("persistKey")
        p <- redis.persist("persistKey")
        ttl2 <- redis.ttl("persistKey")
      } yield {
        s shouldBe true
        e shouldBe true
        ttl.toInt should beBetween(1, 10)
        p shouldBe true
        ttl2 shouldBe -1
      }
      Await.result(r, timeOut)
    }

    "PEXPIRE" in {
      redis.set("pexpireKey", "value").futureValue shouldBe true
      redis.pexpire("pexpireKey", 1000).futureValue shouldBe true
      redis.expire("pexpireKeyNonexisting", 1000).futureValue shouldBe false
      eventually {
        redis.get("pexpireKey").futureValue shouldBe empty
      }
    }

    "PEXPIREAT" in {
      val r = for {
        s <- redis.set("pexpireatKey", "value")
        e <- redis.pexpireat("pexpireatKey", System.currentTimeMillis())
        expired <- redis.get("pexpireatKey")
      } yield {
        s shouldBe true
        e shouldBe true
        expired shouldBe None
      }
      Await.result(r, timeOut)
    }

    "PEXPIREAT TTL" in {
      val r = for {
        s <- redis.set("pttlKey", "value")
        e <- redis.expire("pttlKey", 1)
        pttl <- redis.pttl("pttlKey")
      } yield {
        s shouldBe true
        e shouldBe true
        pttl.toInt should beBetween(1, 1000)
      }
      Await.result(r, timeOut)
    }

    "RANDOMKEY" in {
      val r = for {
        _ <- redis.set("randomKey", "value") // could fail if database was empty
        s <- redis.randomkey()
      } yield {
        s shouldBe defined
      }
      Await.result(r, timeOut)
    }

    "RENAME" in {
      val r = for {
        _ <- redis.del("renameNewKey")
        s <- redis.set("renameKey", "value")
        rename <- redis.rename("renameKey", "renameNewKey")
        renamedValue <- redis.get("renameNewKey")
      } yield {
        s shouldBe true
        rename shouldBe true
        renamedValue shouldBe Some(ByteString("value"))
      }
      Await.result(r, timeOut)
    }

    "RENAMENX" in {
      val r = for {
        _ <- redis.del("renamenxNewKey")
        s <- redis.set("renamenxKey", "value")
        s <- redis.set("renamenxNewKey", "value")
        rename <- redis.renamenx("renamenxKey", "renamenxNewKey")
        _ <- redis.del("renamenxNewKey")
        rename2 <- redis.renamenx("renamenxKey", "renamenxNewKey")
        renamedValue <- redis.get("renamenxNewKey")
      } yield {
        s shouldBe true
        rename shouldBe false
        rename2 shouldBe true
        renamedValue shouldBe Some(ByteString("value"))
      }
      Await.result(r, timeOut)
    }

    "RESTORE" in {
      killDumpIfExists
      val k = "restoreKey"
      val v = "restoreValue"
      val r = for {
        _ <- redis.set(key = k, value = v)
        dump <- redis.dump(k)
        _ <- redis.del(k)
        restore <- redis.restore(k, serializedValue = dump.get)
        restored <- redis.get[String](k)
      } yield {
        restore shouldBe true
        restored shouldBe Some(v)
      }
      Await.result(r, timeOut)
    }

    "SCAN" in {

      withRedisServer(port => {
        val scanRedis = RedisClient("localhost", port)

        val r = for {
          _ <- scanRedis.flushdb()
          _ <- scanRedis.set("scanKey1", "value1")
          _ <- scanRedis.set("scanKey2", "value2")
          _ <- scanRedis.set("scanKey3", "value3")
          result <- scanRedis.scan(count = Some(1000))
        } yield {
          result.index shouldBe 0
          result.data.sorted shouldBe Seq("scanKey1", "scanKey2", "scanKey3")
        }
        Await.result(r, timeOut)
      })
    }

    // @see https://gist.github.com/jacqui/983051
    "SORT" in {
      val init = Future.sequence(
        Seq(
          redis.hset("bonds|1", "bid_price", 96.01),
          redis.hset("bonds|1", "ask_price", 97.53),
          redis.hset("bonds|2", "bid_price", 95.50),
          redis.hset("bonds|2", "ask_price", 98.25),
          redis.del("bond_ids"),
          redis.sadd("bond_ids", 1),
          redis.sadd("bond_ids", 2),
          redis.del("sortAlpha"),
          redis.rpush("sortAlpha", "abc", "xyz")
        ))
      val r = for {
        _ <- init
        sort <- redis.sort("bond_ids")
        sortDesc <- redis.sort("bond_ids", order = Some(DESC))
        sortAlpha <- redis.sort("sortAlpha", alpha = true)
        sortLimit <- redis.sort("bond_ids", limit = Some(LimitOffsetCount(0, 1)))
        b1 <- redis.sort("bond_ids", byPattern = Some("bonds|*->bid_price"))
        b2 <- redis.sort("bond_ids", byPattern = Some("bonds|*->bid_price"), getPatterns = Seq("bonds|*->bid_price"))
        b3 <- redis.sort("bond_ids", Some("bonds|*->bid_price"), getPatterns = Seq("bonds|*->bid_price", "#"))
        b4 <- redis.sort("bond_ids", Some("bonds|*->bid_price"), Some(LimitOffsetCount(0, 1)))
        b5 <- redis.sort("bond_ids", Some("bonds|*->bid_price"), order = Some(DESC))
        b6 <- redis.sort("bond_ids", Some("bonds|*->bid_price"))
        b7 <- redis.sortStore("bond_ids", Some("bonds|*->ask_price"), store = "bond_ids_sorted_by_ask_price")
      } yield {
        sort shouldBe Seq(ByteString("1"), ByteString("2"))
        sortDesc shouldBe Seq(ByteString("2"), ByteString("1"))
        sortAlpha shouldBe Seq(ByteString("abc"), ByteString("xyz"))
        sortLimit shouldBe Seq(ByteString("1"))
        b1 shouldBe Seq(ByteString("2"), ByteString("1"))
        b2 shouldBe Seq(ByteString("95.5"), ByteString("96.01"))
        b3 shouldBe Seq(ByteString("95.5"), ByteString("2"), ByteString("96.01"), ByteString("1"))
        b4 shouldBe Seq(ByteString("2"))
        b5 shouldBe Seq(ByteString("1"), ByteString("2"))
        b6 shouldBe Seq(ByteString("2"), ByteString("1"))
        b7 shouldBe 2
      }
      Await.result(r, timeOut)
    }

    "TTL" in {
      val r = for {
        s <- redis.set("ttlKey", "value")
        e <- redis.expire("ttlKey", 10)
        ttl <- redis.ttl("ttlKey")
      } yield {
        s shouldBe true
        e shouldBe true
        ttl should be >= 1L
        ttl.toInt should beBetween(1, 10)
      }
      Await.result(r, timeOut)
    }

    "TYPE" in {
      val r = for {
        s <- redis.set("typeKey", "value")
        _type <- redis.`type`("typeKey")
        _typeNone <- redis.`type`("typeKeyNonExisting")
      } yield {
        s shouldBe true
        _type shouldBe "string"
        _typeNone shouldBe "none"
      }
      Await.result(r, timeOut)
    }

  }

  private def killDumpIfExists = {
    val fileTemp = new File("dump.rdb")
    if (fileTemp.exists) {
      fileTemp.delete()
    }
  }
}
