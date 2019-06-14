package redis.actors

import akka.actor._
import akka.util.ByteString
import redis.api.hashes.Hgetall
import redis.protocol.MultiBulk

import scala.concurrent.{Await, Promise}
import scala.collection.mutable
import java.net.InetSocketAddress

import com.typesafe.config.ConfigFactory
import redis.{Operation, Redis, TestBase}
import redis.api.connection.Ping
import akka.testkit._

class RedisReplyDecoderSpec
  extends TestKit(ActorSystem("testsystem", ConfigFactory.parseString( """akka.loggers = ["akka.testkit.TestEventListener"]""")))
  with TestBase with ImplicitSender {

  import scala.concurrent.duration._


  val timeout = 5.seconds dilated

  "RedisReplyDecoder" should {
    "ok" in within(timeout){
      val promise = Promise[String]()
      val operation = Operation(Ping, promise)
      val q = QueuePromises(mutable.Queue[Operation[_, _]]())
      q.queue.enqueue(operation)

      val redisReplyDecoder = TestActorRef[RedisReplyDecoder](Props(classOf[RedisReplyDecoder])
          .withDispatcher(Redis.dispatcher.name))

      redisReplyDecoder.underlyingActor.queuePromises shouldBe empty

      redisReplyDecoder ! q
      awaitAssert(redisReplyDecoder.underlyingActor.queuePromises.size shouldBe 1)

      redisReplyDecoder ! ByteString("+PONG\r\n")
      Await.result(promise.future, timeout) shouldBe "PONG"

      awaitAssert(redisReplyDecoder.underlyingActor.queuePromises shouldBe empty)

      val promise2 = Promise[String]()
      val promise3 = Promise[String]()
      val op2 = Operation(Ping, promise2)
      val op3 = Operation(Ping, promise3)
      val q2 = QueuePromises(mutable.Queue[Operation[_, _]]())
      q2.queue.enqueue(op2)
      q2.queue.enqueue(op3)

      redisReplyDecoder ! q2
      awaitAssert(redisReplyDecoder.underlyingActor.queuePromises.size shouldBe 2)

      redisReplyDecoder ! ByteString("+PONG\r\n+PONG\r\n")
      Await.result(promise2.future, timeout) shouldBe "PONG"
      Await.result(promise3.future, timeout) shouldBe "PONG"
      redisReplyDecoder.underlyingActor.queuePromises shouldBe empty
    }

    "can't decode" in within(timeout){
      val probeMock = TestProbe()

      val redisClientActor = TestActorRef[RedisClientActorMock2](
        Props(classOf[RedisClientActorMock2], probeMock.ref).withDispatcher(Redis.dispatcher.name))
      val promise = Promise[String]()
      redisClientActor ! Operation(Ping, promise)
      awaitAssert({
        redisClientActor.underlyingActor.queuePromises.length shouldBe 1
        redisClientActor.underlyingActor.onWriteSent()
        redisClientActor.underlyingActor.queuePromises shouldBe empty
      }, timeout)

      EventFilter[Exception](occurrences = 1, start = "Redis Protocol error: Got 110 as initial reply byte").intercept({
        redisClientActor.underlyingActor.onDataReceived(ByteString("not valid redis reply"))
      })
      probeMock.expectMsg("restartConnection") shouldBe "restartConnection"
       a[InvalidRedisReply.type] should be thrownBy  Await.result(promise.future, timeout)


      val promise2 = Promise[String]()
      redisClientActor ! Operation(Ping, promise2)
      awaitAssert({
        redisClientActor.underlyingActor.queuePromises.length shouldBe 1
        redisClientActor.underlyingActor.onWriteSent()
        redisClientActor.underlyingActor.queuePromises shouldBe empty
      }, timeout)

      EventFilter[Exception](occurrences = 1, start = "Redis Protocol error: Got 110 as initial reply byte").intercept({
        redisClientActor.underlyingActor.onDataReceived(ByteString("not valid redis reply"))
      })
      probeMock.expectMsg("restartConnection") shouldBe "restartConnection"
      a[InvalidRedisReply.type] should be thrownBy  Await.result(promise2.future, timeout)
    }

    "redis reply in many chunks" in within(timeout){
      val promise1 = Promise[String]()
      val promise2 = Promise[String]()
      val promise3 = Promise[Map[String, String]]()
      val operation1 = Operation(Ping, promise1)
      val operation2 = Operation(Ping, promise2)
      val operation3 = Operation[MultiBulk, Map[String, String]](Hgetall[String, String]("key"), promise3)
      val q = QueuePromises(mutable.Queue[Operation[_, _]]())
      q.queue.enqueue(operation1)
      q.queue.enqueue(operation2)
      q.queue.enqueue(operation3)

      val redisReplyDecoder = TestActorRef[RedisReplyDecoder](Props(classOf[RedisReplyDecoder])
          .withDispatcher(Redis.dispatcher.name))

      redisReplyDecoder.underlyingActor.queuePromises shouldBe empty

      redisReplyDecoder ! q
      awaitAssert({
        redisReplyDecoder.underlyingActor.queuePromises.size shouldBe 3
      }, timeout)

      redisReplyDecoder ! ByteString("+P")
      awaitAssert(redisReplyDecoder.underlyingActor.partiallyDecoded.rest == ByteString("+P"))
      awaitAssert(redisReplyDecoder.underlyingActor.partiallyDecoded.isFullyDecoded shouldBe false)

      redisReplyDecoder ! ByteString("ONG\r")
      awaitAssert(redisReplyDecoder.underlyingActor.partiallyDecoded.rest == ByteString("+PONG\r"))

      redisReplyDecoder ! ByteString("\n+PONG2")
      awaitAssert(redisReplyDecoder.underlyingActor.partiallyDecoded.rest == ByteString("+PONG2"))

      Await.result(promise1.future, timeout) shouldBe "PONG"

      redisReplyDecoder ! ByteString("\r\n")
      awaitAssert(redisReplyDecoder.underlyingActor.partiallyDecoded.isFullyDecoded shouldBe true)
      awaitAssert(redisReplyDecoder.underlyingActor.partiallyDecoded.rest.isEmpty shouldBe true)

      Await.result(promise2.future, timeout) shouldBe "PONG2"

      awaitAssert(redisReplyDecoder.underlyingActor.queuePromises.size shouldBe 1)

      val multibulkString0 = ByteString()
      val multibulkString = ByteString("*4\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$5\r\nHello\r\n$5\r\nWorld\r\n")
      val (multibulkStringStart, multibulkStringEnd) = multibulkString.splitAt(multibulkString.length - 1)

      redisReplyDecoder ! multibulkString0
      awaitAssert(redisReplyDecoder.underlyingActor.partiallyDecoded.isFullyDecoded shouldBe true, timeout)

      for {
        b <- multibulkStringStart
      } yield {
        redisReplyDecoder ! ByteString(b)
        awaitAssert(redisReplyDecoder.underlyingActor.partiallyDecoded.isFullyDecoded shouldBe false, timeout)
      }
      redisReplyDecoder ! multibulkStringEnd

      awaitAssert(redisReplyDecoder.underlyingActor.queuePromises.size shouldBe 0)
      awaitAssert(redisReplyDecoder.underlyingActor.partiallyDecoded.isFullyDecoded shouldBe true, timeout)

      Await.result(promise3.future, timeout) shouldBe Map("foo" -> "bar", "Hello" -> "World")

      redisReplyDecoder.underlyingActor.queuePromises shouldBe empty
    }
  }
}

class RedisClientActorMock2(probeMock: ActorRef)
  extends RedisClientActor(new InetSocketAddress("localhost", 6379), () => {Seq()}, (status:Boolean) => {()}, Redis.dispatcher.name) {
  override def preStart(): Unit = {
    // disable preStart of RedisWorkerIO
  }

  override def restartConnection(): Unit = {
    super.restartConnection()
    probeMock ! "restartConnection"
  }
}
