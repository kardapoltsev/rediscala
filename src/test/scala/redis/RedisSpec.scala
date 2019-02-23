package redis

import java.io.{InputStream, OutputStream}
import java.net.Socket
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.apache.logging.log4j.scala.Logger
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.io.Source
import scala.reflect.io.File
import scala.sys.process.{ProcessIO, _}
import scala.util.Try
import scala.util.control.NonFatal

object RedisServerHelper {
  val redisHost = "127.0.0.1"

  // remove stacktrace when we stop the process
  val redisServerCmd = "redis-server"
  val redisCliCmd = "redis-cli"
  val redisServerLogLevel = ""

  val portNumber = new AtomicInteger(10500)
}

abstract class RedisHelper extends TestKit(ActorSystem()) with TestBase with BeforeAndAfterAll {
  protected val processLogger = ProcessLogger(line => log.debug(line), line => log.error(line))

  import scala.concurrent.duration._

  implicit val executionContext = system.dispatchers.lookup(Redis.dispatcher.name)

  override def spanScaleFactor: Double = {
    testKitSettings.TestTimeFactor
  }


  val timeOut = 10.seconds

  override protected def beforeAll(): Unit = {
    setup()
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    cleanup()
  }

  def setup()

  def cleanup()

  class RedisManager {

    import RedisServerHelper._

    var processes: Seq[RedisProcess] = Seq.empty

    def newSentinelProcess(masterName: String, masterPort: Int, port: Int = portNumber.incrementAndGet()) = {
      startProcess(new SentinelProcess(masterName, masterPort, port))
    }

    def newSlaveProcess(masterPort: Int, port: Int = portNumber.incrementAndGet()) = {
      startProcess(new SlaveProcess(masterPort, port))
    }

    def newRedisProcess(port: Int = portNumber.incrementAndGet()) = {
      startProcess(new RedisProcess(port))
    }

    private def startProcess(process: RedisProcess): RedisProcess = {
      process.start()
      ensureRedisStarted(redisHost, process.port)
      processes = processes :+ process
      process
    }

    def stopAll() = {
      processes.foreach(_.stop())
    }

  }

  protected def ensureRedisStarted(host: String, port: Int): Unit = {
    val client = RedisClient(host, port)
    eventually {
      client.ping().futureValue
    }
  }

}

case class RedisVersion(major: Int, minor: Int, patch: Int) extends Ordered[RedisVersion] {

  import scala.math.Ordered.orderingToOrdered

  override def compare(that: RedisVersion): Int =
    (this.major, this.minor, this.patch)
      .compare((that.major, that.minor, that.patch))
}

abstract class RedisStandaloneServer extends RedisHelper {

  val redisManager = new RedisManager()

  val server = redisManager.newRedisProcess()

  val port = server.port
  lazy val redis = RedisClient(port = port)

  def redisVersion(): Future[Option[RedisVersion]] = redis.info("Server").map { info =>
    info
      .split("\r\n")
      .drop(1)
      .flatMap { line =>
        line.split(":") match {
          case Array(key, value) => List(key -> value)
          case _                 => List.empty
        }
      }
      .find(_._1 == "redis_version")
      .map(_._2.split("\\.") match {
        case Array(major, minor, patch) =>
          RedisVersion(major.toInt, minor.toInt, patch.toInt)
      })
  }

  def withRedisServer[T](block: (Int) => T): T = {
    val serverProcess = redisManager.newRedisProcess()
    val result = Try(block(serverProcess.port))
    serverProcess.stop()
    result.get
  }

  override def setup() = {}

  override def cleanup() = {
    redisManager.stopAll()
  }
}

abstract class RedisSentinelClients(val masterName: String = "mymaster") extends RedisHelper {

  import RedisServerHelper._

  val masterPort = portNumber.incrementAndGet()
  val slavePort1 = portNumber.incrementAndGet()
  val slavePort2 = portNumber.incrementAndGet()
  val sentinelPort1 = portNumber.incrementAndGet()
  val sentinelPort2 = portNumber.incrementAndGet()

  val sentinelPorts = Seq(sentinelPort1, sentinelPort2)

  lazy val redisClient = RedisClient(port = masterPort)
  lazy val sentinelClient = SentinelClient(port = sentinelPort1)
  lazy val sentinelMonitoredRedisClient =
    SentinelMonitoredRedisClient(
      master = masterName,
      sentinels = Seq((redisHost, sentinelPort1), (redisHost, sentinelPort2)))

  val redisManager = new RedisManager()

  val master = redisManager.newRedisProcess(masterPort)
  val slave1 = redisManager.newSlaveProcess(masterPort, slavePort1)
  val slave2 = redisManager.newSlaveProcess(masterPort, slavePort2)

  val sentinel1 =
    redisManager.newSentinelProcess(masterName, masterPort, sentinelPort1)
  val sentinel2 =
    redisManager.newSentinelProcess(masterName, masterPort, sentinelPort2)

  def newSlaveProcess() = {
    redisManager.newSlaveProcess(masterPort)
  }

  def newSentinelProcess() = {
    redisManager.newSentinelProcess(masterName, masterPort)
  }

  override def setup() = {}

  override def cleanup() = {
    redisManager.stopAll()
  }

}

abstract class RedisClusterClients() extends RedisHelper {

  import RedisServerHelper._

  var processes: Seq[Process] = Seq.empty
  protected val fileDir = createTempDirectory().toFile

  def newNode(port: Int) =
    s"$redisServerCmd --port $port --cluster-enabled yes --cluster-config-file nodes-${port}.conf --cluster-node-timeout 30000 --appendonly yes --appendfilename appendonly-${port}.aof --dbfilename dump-${port}.rdb --logfile ${port}.log --daemonize yes"

  val nodePorts = (0 to 5).map(_ => portNumber.incrementAndGet())

  override def setup() = {
    println("Setup")
    fileDir.mkdirs()

    processes = nodePorts.map(s => Process(newNode(s), fileDir).run(processLogger))
    val nodes = nodePorts.map(s => redisHost + ":" + s).mkString(" ")

    val createClusterCmd =
      s"$redisCliCmd --cluster create --cluster-replicas 1 ${nodes}"
    println(createClusterCmd)
    Process(createClusterCmd)
      .run(
        new ProcessIO(
          (writeInput: OutputStream) => {
//            Thread.sleep(2000)
            println("yes")
            writeInput.write("yes\n".getBytes)
            writeInput.flush
          },
          (processOutput: InputStream) => {
            Source.fromInputStream(processOutput).getLines().foreach { l =>
              println(l)
            }
          },
          (processError: InputStream) => {
            Source.fromInputStream(processError).getLines().foreach { l =>
              println(l)
            }
          },
          daemonizeThreads = false
        )
      )
      .exitValue()

    val servers = nodePorts.map { port => RedisServer(host = redisHost, port = port)}
    val client = RedisCluster(servers)
    eventually {
      val clusterInfo = client.clusterInfo().futureValue
      clusterInfo("cluster_known_nodes") shouldBe nodePorts.length.toString
    }
    client.stop()
  }

  override def cleanup() = {
    println("Stop begin")

    nodePorts foreach { port =>
      val out = new Socket(redisHost, port).getOutputStream
      out.write("SHUTDOWN NOSAVE\n".getBytes)
      out.flush()
    }
    //Await.ready(RedisClient(port = port).shutdown(redis.api.NOSAVE),timeOut) }
    processes.foreach(_.destroy())

    //deleteDirectory()

    println("Stop end")
  }

  def deleteDirectory(): Unit = {
    val fileStream = Files.newDirectoryStream(fileDir.toPath)
    fileStream.iterator().asScala.foreach(Files.delete)
    Files.delete(fileDir.toPath)
  }
}

import redis.RedisServerHelper._

class RedisProcess(val port: Int) {
  var server: Process = null
  val cmd = s"${redisServerCmd} --port $port ${redisServerLogLevel}"
  protected val log = Logger(getClass)
  protected val processLogger = ProcessLogger(line => log.debug(line), line => log.error(line))

  def start() = {
    if (server == null)
      server = Process(cmd).run(processLogger)
  }

  def stop() = {
    if (server != null) {
      try {
        val out = new Socket(redisHost, port).getOutputStream
        out.write("SHUTDOWN NOSAVE\n".getBytes)
        out.flush
        out.close()

      } catch {
        case NonFatal(e) => e.printStackTrace()
      } finally {
        server.destroy()
        server = null
      }
    }

  }
}

class SentinelProcess(masterName: String, masterPort: Int, port: Int) extends RedisProcess(port) {

  lazy val sentinelConfPath = {
    val sentinelConf =
      s"""
         |sentinel monitor $masterName $redisHost $masterPort 2
         |sentinel down-after-milliseconds $masterName 5000
         |sentinel parallel-syncs $masterName 1
         |sentinel failover-timeout $masterName 10000
            """.stripMargin

    val sentinelConfFile = File.makeTemp("rediscala-sentinel", ".conf")
    sentinelConfFile.writeAll(sentinelConf)
    sentinelConfFile.path
  }

  override val cmd =
    s"${redisServerCmd} $sentinelConfPath --port $port --sentinel $redisServerLogLevel"
}

class SlaveProcess(masterPort: Int, port: Int) extends RedisProcess(port) {
  override val cmd =
    s"$redisServerCmd --port $port --slaveof $redisHost $masterPort $redisServerLogLevel"
}
