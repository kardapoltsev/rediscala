package redis.commands

import akka.util.ByteString
import redis._

import scala.concurrent.Await

class ListsSpec extends RedisStandaloneServer {

  "Lists commands" should {

    "LINDEX" in {
      val r = for {
        _ <- redis.del("lindexKey")
        _ <- redis.lpush("lindexKey", "World", "Hello")
        hello <- redis.lindex("lindexKey", 0)
        world <- redis.lindex("lindexKey", 1)
        none <- redis.lindex("lindexKey", 2)
      } yield {
        hello shouldBe Some(ByteString("Hello"))
        world shouldBe Some(ByteString("World"))
        none shouldBe None
      }
      Await.result(r, timeOut)
    }

    "LINSERT" in {
      val r = for {
        _ <- redis.del("linsertKey")
        _ <- redis.lpush("linsertKey", "World", "Hello")
        length <- redis.linsertBefore("linsertKey", "World", "There")
        list <- redis.lrange("linsertKey", 0, -1)
        length4 <- redis.linsertAfter("linsertKey", "World", "!!!")
        list4 <- redis.lrange("linsertKey", 0, -1)
      } yield {
        length shouldBe 3
        list shouldBe Seq(ByteString("Hello"), ByteString("There"), ByteString("World"))
        length4 shouldBe 4
        list4 shouldBe Seq(ByteString("Hello"), ByteString("There"), ByteString("World"), ByteString("!!!"))
      }
      Await.result(r, timeOut)
    }

    "LLEN" in {
      val r = for {
        _ <- redis.del("llenKey")
        _ <- redis.lpush("llenKey", "World", "Hello")
        length <- redis.llen("llenKey")
      } yield {
        length shouldBe 2
      }
      Await.result(r, timeOut)
    }

    "LPOP" in {
      val r = for {
        _ <- redis.del("lpopKey")
        _ <- redis.rpush("lpopKey", "one", "two", "three")
        e <- redis.lpop("lpopKey")
      } yield {
        e shouldBe Some(ByteString("one"))
      }
      Await.result(r, timeOut)
    }

    "LPUSH" in {
      val r = for {
        _ <- redis.del("lpushKey")
        _ <- redis.lpush("lpushKey", "World", "Hello")
        list <- redis.lrange("lpushKey", 0, -1)
      } yield {
        list shouldBe Seq(ByteString("Hello"), ByteString("World"))
      }
      Await.result(r, timeOut)
    }

    "LPUSHX" in {
      val r = for {
        _ <- redis.del("lpushxKey")
        _ <- redis.del("lpushxKeyOther")
        i <- redis.rpush("lpushxKey", "world")
        ii <- redis.lpushx("lpushxKey", "hello")
        zero <- redis.lpushx("lpushxKeyOther", "hello")
        list <- redis.lrange("lpushxKey", 0, -1)
        listOther <- redis.lrange("lpushxKeyOther", 0, -1)
      } yield {
        i shouldBe 1
        ii shouldBe 2
        zero shouldBe 0
        list shouldBe Seq(ByteString("hello"), ByteString("world"))
        listOther shouldBe empty
      }
      Await.result(r, timeOut)
    }

    "LRANGE" in {
      val r = for {
        _ <- redis.del("lrangeKey")
        _ <- redis.rpush("lrangeKey", "one", "two", "three")
        list <- redis.lrange("lrangeKey", 0, 0)
        list2 <- redis.lrange("lrangeKey", -3, 2)
        list3 <- redis.lrange("lrangeKey", 5, 10)
        nonExisting <- redis.lrange("lrangeKeyNonexisting", 5, 10)
      } yield {
        list shouldBe Seq(ByteString("one"))
        list2 shouldBe Seq(ByteString("one"), ByteString("two"), ByteString("three"))
        list3 shouldBe empty
        nonExisting shouldBe empty
      }
      Await.result(r, timeOut)
    }

    "LREM" in {
      val r = for {
        _ <- redis.del("lremKey")
        _ <- redis.rpush("lremKey", "hello", "hello", "foo", "hello")
        lrem <- redis.lrem("lremKey", -2, "hello")
        list2 <- redis.lrange("lremKey", 0, -1)
      } yield {
        lrem shouldBe 2
        list2 shouldBe Seq(ByteString("hello"), ByteString("foo"))
      }
      Await.result(r, timeOut)
    }

    "LSET" in {
      val r = for {
        _ <- redis.del("lsetKey")
        _ <- redis.rpush("lsetKey", "one", "two", "three")
        lset1 <- redis.lset("lsetKey", 0, "four")
        lset2 <- redis.lset("lsetKey", -2, "five")
        list <- redis.lrange("lsetKey", 0, -1)
      } yield {
        lset1 shouldBe true
        lset2 shouldBe true
        list shouldBe Seq(ByteString("four"), ByteString("five"), ByteString("three"))
      }
      Await.result(r, timeOut)
    }

    "LTRIM" in {
      val r = for {
        _ <- redis.del("ltrimKey")
        _ <- redis.rpush("ltrimKey", "one", "two", "three")
        ltrim <- redis.ltrim("ltrimKey", 1, -1)
        list <- redis.lrange("ltrimKey", 0, -1)
      } yield {
        ltrim shouldBe true
        list shouldBe Seq(ByteString("two"), ByteString("three"))
      }
      Await.result(r, timeOut)
    }

    "RPOP" in {
      val r = for {
        _ <- redis.del("rpopKey")
        _ <- redis.rpush("rpopKey", "one", "two", "three")
        rpop <- redis.rpop("rpopKey")
        list <- redis.lrange("rpopKey", 0, -1)
      } yield {
        rpop shouldBe Some(ByteString("three"))
        list shouldBe Seq(ByteString("one"), ByteString("two"))
      }
      Await.result(r, timeOut)
    }

    "RPOPLPUSH" in {
      val r = for {
        _ <- redis.del("rpoplpushKey")
        _ <- redis.del("rpoplpushKeyOther")
        _ <- redis.rpush("rpoplpushKey", "one", "two", "three")
        rpoplpush <- redis.rpoplpush("rpoplpushKey", "rpoplpushKeyOther")
        list <- redis.lrange("rpoplpushKey", 0, -1)
        listOther <- redis.lrange("rpoplpushKeyOther", 0, -1)
      } yield {
        rpoplpush shouldBe Some(ByteString("three"))
        list shouldBe Seq(ByteString("one"), ByteString("two"))
        listOther shouldBe Seq(ByteString("three"))
      }
      Await.result(r, timeOut)
    }

    "RPUSH" in {
      val r = for {
        _ <- redis.del("rpushKey")
        i <- redis.rpush("rpushKey", "hello")
        ii <- redis.rpush("rpushKey", "world")
        list <- redis.lrange("rpushKey", 0, -1)
      } yield {
        i shouldBe 1
        ii shouldBe 2
        list shouldBe Seq(ByteString("hello"), ByteString("world"))
      }
      Await.result(r, timeOut)
    }

    "RPUSHX" in {
      val r = for {
        _ <- redis.del("rpushxKey")
        _ <- redis.del("rpushxKeyOther")
        i <- redis.rpush("rpushxKey", "hello")
        ii <- redis.rpushx("rpushxKey", "world")
        zero <- redis.rpushx("rpushxKeyOther", "world")
        list <- redis.lrange("rpushxKey", 0, -1)
        listOther <- redis.lrange("rpushxKeyOther", 0, -1)
      } yield {
        i shouldBe 1
        ii shouldBe 2
        zero shouldBe 0
        list shouldBe Seq(ByteString("hello"), ByteString("world"))
        listOther shouldBe empty
      }
      Await.result(r, timeOut)
    }
  }
}
