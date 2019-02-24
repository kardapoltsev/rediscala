package redis

import java.net.Socket

import org.apache.logging.log4j.scala.Logger
import redis.RedisServerHelper.{redisHost, redisServerCmd, redisServerLogLevel}

import scala.reflect.io.File
import scala.sys.process.{Process, ProcessLogger}
import scala.util.control.NonFatal

class RedisProcess(val port: Int) {
  var server: Process         = null
  val cmd                     = s"${redisServerCmd} --port $port ${redisServerLogLevel}"
  protected val log           = Logger(getClass)
  protected val processLogger = ProcessLogger(line => log.debug(line), line => log.error(line))

  def start(): Unit = {
    log.debug(s"starting $this")
    if (server == null)
      server = Process(cmd).run(processLogger)
  }

  def stop(): Unit = {
    log.debug(s"stopping $this")
    if (server != null) {
      try {
        val out = new Socket(redisHost, port).getOutputStream
        out.write("SHUTDOWN NOSAVE\n".getBytes)
        out.flush()
        out.close()
      } catch {
        case NonFatal(e) => log.error(s"couldn't stop $this", e)
      } finally {
        server.destroy()
        server = null
      }
    }
  }

  override def toString: String = s"RedisProcess($port)"
}

class SentinelProcess(masterName: String, masterPort: Int, port: Int) extends RedisProcess(port) {
  val sentinelConfPath = {
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

  override val cmd = s"${redisServerCmd} $sentinelConfPath --port $port --sentinel $redisServerLogLevel"
}

class SlaveProcess(masterPort: Int, port: Int) extends RedisProcess(port) {
  override val cmd = s"$redisServerCmd --port $port --slaveof $redisHost $masterPort $redisServerLogLevel"
}
