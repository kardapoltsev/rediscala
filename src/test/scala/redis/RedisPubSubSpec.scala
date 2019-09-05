package redis

import redis.api.pubsub._
import redis.actors.RedisSubscriberActor
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, Props}
import akka.testkit.{TestActorRef, TestProbe}
import akka.util.ByteString

class RedisPubSubSpec extends RedisStandaloneServer {


  "PubSub test" should {
    "ok (client + callback)" in {
      val receivedMessages = new AtomicInteger()
      val channel1 = "ch1"
      val channel2 = "ch2"
      RedisPubSub(
        port = port,
        channels = Seq(channel1, channel2),
        patterns = Nil,
        onMessage = (m: Message) => {
          log.debug(s"received $m")
          if(m.channel != channel2) {
            receivedMessages.incrementAndGet()
          }
        }
      )

      //wait for subscription
      eventually {
        redis.publish(channel2, "1").futureValue shouldBe 1
      }

      eventually {
        redis.publish(channel1, "2").futureValue shouldBe 1
        redis.publish("otherChannel", "message").futureValue shouldBe 0
        receivedMessages.get() shouldBe 1
      }
    }

    "ok (actor)" in {
      val probeMock = TestProbe()
      val channels = Seq("channel")
      val patterns = Seq("pattern.*")

      val subscriberActor = TestActorRef[SubscriberActor](
        Props(classOf[SubscriberActor], new InetSocketAddress("localhost", port),
          channels, patterns, probeMock.ref)
          .withDispatcher(Redis.dispatcher.name),
        "SubscriberActor"
      )
      import scala.concurrent.duration._

      system.scheduler.scheduleOnce(2 seconds)(redis.publish("channel", "value"))

      probeMock.expectMsgType[Message](5 seconds) shouldBe Message("channel", ByteString("value"))

      redis.publish("pattern.1", "value")

      probeMock.expectMsgType[PMessage] shouldBe PMessage("pattern.*", "pattern.1", ByteString("value"))

      subscriberActor.underlyingActor.subscribe("channel2")
      subscriberActor.underlyingActor.unsubscribe("channel")

      system.scheduler.scheduleOnce(2 seconds)({
        redis.publish("channel", "value")
        redis.publish("channel2", "value")
      })
      probeMock.expectMsgType[Message](5 seconds) shouldBe Message("channel2", ByteString("value"))

      subscriberActor.underlyingActor.unsubscribe("channel2")
      system.scheduler.scheduleOnce(1 second)({
        redis.publish("channel2", ByteString("value"))
      })
      probeMock.expectNoMessage(3 seconds)

      subscriberActor.underlyingActor.subscribe("channel2")
      system.scheduler.scheduleOnce(1 second)({
        redis.publish("channel2", ByteString("value"))
      })
      probeMock.expectMsgType[Message](5 seconds) shouldBe Message("channel2", ByteString("value"))

      subscriberActor.underlyingActor.psubscribe("pattern2.*")
      subscriberActor.underlyingActor.punsubscribe("pattern.*")

      system.scheduler.scheduleOnce(2 seconds)({
        redis.publish("pattern2.match", ByteString("value"))
        redis.publish("pattern.*", ByteString("value"))
      })
      probeMock.expectMsgType[PMessage](5 seconds) shouldBe PMessage("pattern2.*", "pattern2.match", ByteString("value"))

      subscriberActor.underlyingActor.punsubscribe("pattern2.*")
      system.scheduler.scheduleOnce(2 seconds)({
        redis.publish("pattern2.match", ByteString("value"))
      })
      probeMock.expectNoMessage(3 seconds)

      subscriberActor.underlyingActor.psubscribe("pattern.*")
      system.scheduler.scheduleOnce(2 seconds)({
        redis.publish("pattern.*", ByteString("value"))
      })
      probeMock.expectMsgType[PMessage](5 seconds) shouldBe PMessage("pattern.*", "pattern.*", ByteString("value"))
    }
  }

}

class SubscriberActor(address: InetSocketAddress,
                      channels: Seq[String],
                      patterns: Seq[String],
                      probeMock: ActorRef
                       ) extends RedisSubscriberActor(address, channels, patterns, None, (b:Boolean) => () ) {

  override def onMessage(m: Message) = {
    probeMock ! m
  }

  def onPMessage(pm: PMessage) {
    probeMock ! pm
  }
}


