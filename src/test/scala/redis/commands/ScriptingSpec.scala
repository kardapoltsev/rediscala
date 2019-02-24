package redis.commands

import java.io.File

import redis._

import akka.util.ByteString
import redis.protocol.{Bulk, MultiBulk}
import redis.actors.ReplyErrorException
import redis.api.scripting.RedisScript

class ScriptingSpec extends RedisStandaloneServer {

  "Scripting commands" should {
    val redisScript                 = RedisScript("return 'rediscala'")
    val redisScriptKeysArgs         = RedisScript("return {KEYS[1],ARGV[1]}")
    val redisScriptConversionObject = RedisScript("return redis.call('get', 'dumbKey')")

    "evalshaOrEval (RedisScript)" in {
      redis.scriptFlush().futureValue shouldBe true
      val r = redis.evalshaOrEval(redisScriptKeysArgs, Seq("key"), Seq("arg")).futureValue
      r shouldBe MultiBulk(Some(Vector(Bulk(Some(ByteString("key"))), Bulk(Some(ByteString("arg"))))))
    }

    "EVAL" in {
      redis.eval(redisScript.script).futureValue shouldBe Bulk(Some(ByteString("rediscala")))
    }

    "EVAL with type conversion" in {
      val dumbObject = new DumbClass("foo", "bar")
      val r = redis
        .set("dumbKey", dumbObject)
        .flatMap(_ => {
          redis.eval[DumbClass](redisScriptConversionObject.script)
        })

      r.futureValue shouldBe dumbObject
    }

    "EVALSHA" in {
      redis.evalsha(redisScript.sha1).futureValue shouldBe Bulk(Some(ByteString("rediscala")))
    }

    "EVALSHA with type conversion" in {
      val dumbObject = new DumbClass("foo2", "bar2")
      val r = redis
        .set("dumbKey", dumbObject)
        .flatMap(_ => {
          redis.evalsha[DumbClass](redisScriptConversionObject.sha1)
        })

      r.futureValue shouldBe dumbObject
    }

    "evalshaOrEvalForTypeOf (RedisScript)" in {
      redis.scriptFlush().futureValue shouldBe true
      val dumbObject = new DumbClass("foo3", "bar3")

      val r = redis
        .set("dumbKey", dumbObject)
        .flatMap(_ => {
          redis.evalshaOrEval[DumbClass](redisScriptConversionObject)
        })

      r.futureValue shouldBe dumbObject
    }

    "SCRIPT FLUSH" in {
      redis.scriptFlush().futureValue shouldBe true
    }

    "SCRIPT KILL" in {

      withRedisServer(serverPort => {
        val redisKiller         = RedisClient(port = serverPort)
        val redisScriptLauncher = RedisClient(port = serverPort)
        redisKiller.scriptKill().failed.futureValue shouldBe a[ReplyErrorException]

        val infiniteScript = redisScriptLauncher.eval("""
            |local i = 1
            |while(i > 0) do
            |end
            |return 0
          """.stripMargin)
        Thread.sleep(500)
        eventually {
          redisKiller.scriptKill().futureValue shouldBe true
        }
        infiniteScript.failed.futureValue
      })
    }

    "SCRIPT LOAD" in {
      redis.scriptLoad("return 'rediscala'").futureValue shouldBe "d4cf7650161a37eb55a7e9325f3534cec6fc3241"
    }

    "SCRIPT EXISTS" in {
      val redisScriptNotFound = RedisScript("return 'SCRIPT EXISTS not found'")
      val redisScriptFound    = RedisScript("return 'SCRIPT EXISTS found'")
      val scriptsLoaded = redis
        .scriptLoad(redisScriptFound.script)
        .flatMap(_ => redis.scriptExists(redisScriptFound.sha1, redisScriptNotFound.sha1))
      scriptsLoaded.futureValue shouldBe Seq(true, false)

    }

    "fromFile" in {
      val testScriptFile = new File(getClass.getResource("/lua/test.lua").getPath)
      RedisScript.fromFile(testScriptFile) shouldBe RedisScript("""return "test"""")
    }

    "fromResource" in {
      val testScriptPath = "/lua/test.lua"
      RedisScript.fromResource(testScriptPath) shouldBe RedisScript("""return "test"""")
    }

  }
}
