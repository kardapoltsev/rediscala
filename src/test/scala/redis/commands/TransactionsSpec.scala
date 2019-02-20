package redis.commands

import redis._
import scala.concurrent.Await
import akka.util.ByteString
import redis.actors.ReplyErrorException
import redis.protocol.{Bulk, Status, MultiBulk}

class TransactionsSpec extends RedisStandaloneServer {

  "Transactions commands" should {
    "basic" in {
      val redisTransaction = redis.transaction()
      redisTransaction.exec()
      redisTransaction.watch("a")
      val set = redisTransaction.set("a", "abc")
      val decr = redisTransaction.decr("a")
      val get = redisTransaction.get("a")
      redisTransaction.exec()
      val r = for {
        s <- set
        g <- get
      } yield {
        s shouldBe true
        g shouldBe Some(ByteString("abc"))
      }
      the[ReplyErrorException] thrownBy {
        Await.result(decr, timeOut)
      }
      Await.result(r, timeOut)
    }

    "function api" in {
      withClue("empty") {
        val empty = redis.multi().exec()
        Await.result(empty, timeOut) shouldBe MultiBulk(Some(Vector()))
      }
      val redisTransaction = redis.multi(redis => {
        redis.set("a", "abc")
        redis.get("a")
      })
      withClue("non empty") {
        val exec = redisTransaction.exec()
        Await.result(exec, timeOut) shouldBe MultiBulk(Some(Vector(Status(ByteString("OK")), Bulk(Some(ByteString("abc"))))))
      }
      withClue("reused") {
        redisTransaction.get("transactionUndefinedKey")
        val exec = redisTransaction.exec()
        Await.result(exec, timeOut) shouldBe MultiBulk(Some(Vector(Status(ByteString("OK")), Bulk(Some(ByteString("abc"))), Bulk(None))))
      }
      withClue("watch") {
        val transaction = redis.watch("transactionWatchKey")
        transaction.watcher.result() shouldBe Set("transactionWatchKey")
        transaction.unwatch()
        transaction.watcher.result() shouldBe empty
        val set = transaction.set("transactionWatch", "value")
        transaction.exec()
        val r = for {
          s <- set
        } yield {
          s shouldBe true
        }
        Await.result(r, timeOut)
      }
    }

  }
}
