package redis.commands

import java.io.File

import redis._

import scala.concurrent.Await
import akka.util.ByteString
import redis.protocol.{Bulk, MultiBulk}
import redis.actors.ReplyErrorException
import redis.api.scripting.RedisScript

class ScriptingSpec extends RedisStandaloneServer {

  "Scripting commands" should {
    val redisScript = RedisScript("return 'rediscala'")
    val redisScriptKeysArgs = RedisScript("return {KEYS[1],ARGV[1]}")
    val redisScriptConversionObject = RedisScript("return redis.call('get', 'dumbKey')")

    "evalshaOrEval (RedisScript)" in {
      Await.result(redis.scriptFlush(), timeOut) shouldBe true
      val r = Await.result(redis.evalshaOrEval(redisScriptKeysArgs, Seq("key"), Seq("arg")), timeOut)
      r shouldBe MultiBulk(Some(Vector(Bulk(Some(ByteString("key"))), Bulk(Some(ByteString("arg"))))))
    }

    "EVAL" in {
      Await.result(redis.eval(redisScript.script), timeOut) shouldBe Bulk(Some(ByteString("rediscala")))
    }

    "EVAL with type conversion" in {
      val dumbObject = new DumbClass("foo", "bar")
      val r = redis.set("dumbKey", dumbObject).flatMap(_ => {
        redis.eval[DumbClass](redisScriptConversionObject.script)
      })

      Await.result(r, timeOut) shouldBe dumbObject
    }

    "EVALSHA" in {
      Await.result(redis.evalsha(redisScript.sha1), timeOut) shouldBe Bulk(Some(ByteString("rediscala")))
    }

    "EVALSHA with type conversion" in {
      val dumbObject = new DumbClass("foo2", "bar2")
      val r = redis.set("dumbKey", dumbObject).flatMap(_ => {
        redis.evalsha[DumbClass](redisScriptConversionObject.sha1)
      })

      Await.result(r, timeOut) shouldBe dumbObject
    }

    "evalshaOrEvalForTypeOf (RedisScript)" in {
      Await.result(redis.scriptFlush(), timeOut) shouldBe true
      val dumbObject = new DumbClass("foo3", "bar3")

      val r = redis.set("dumbKey", dumbObject).flatMap(_ => {
        redis.evalshaOrEval[DumbClass](redisScriptConversionObject)
      })

      Await.result(r, timeOut) shouldBe dumbObject
    }

    "SCRIPT FLUSH" in {
      Await.result(redis.scriptFlush(), timeOut) shouldBe true
    }

    "SCRIPT KILL" in {

      withRedisServer(serverPort => {
        val redisKiller = RedisClient(port = serverPort)
        val redisScriptLauncher = RedisClient(port = serverPort)
         a[ReplyErrorException] should be thrownBy {
           Await.result(redisKiller.scriptKill(), timeOut)
         }

        val infiniteScript = redisScriptLauncher.eval(
          """
            |local i = 1
            |while(i > 0) do
            |end
            |return 0
          """.stripMargin)
        Thread.sleep(500)
        eventually {
          redisKiller.scriptKill().futureValue shouldBe true
        }
        a[ReplyErrorException] should be thrownBy {
          Await.result(infiniteScript, timeOut)
        }
      })
    }

    "SCRIPT LOAD" in {
      Await.result(redis.scriptLoad("return 'rediscala'"), timeOut) shouldBe "d4cf7650161a37eb55a7e9325f3534cec6fc3241"
    }

    "SCRIPT EXISTS" in {
      val redisScriptNotFound = RedisScript("return 'SCRIPT EXISTS not found'")
      val redisScriptFound = RedisScript("return 'SCRIPT EXISTS found'")
      val scriptsLoaded = redis.scriptLoad(redisScriptFound.script).flatMap(_ =>
        redis.scriptExists(redisScriptFound.sha1, redisScriptNotFound.sha1)
      )
      Await.result(scriptsLoaded, timeOut) shouldBe Seq(true, false)

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
