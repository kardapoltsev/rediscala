package redis

import java.net.Socket

import org.apache.logging.log4j.scala.Logger
import redis.RedisServerHelper.{redisHost, redisServerCmd, redisServerLogLevel}

import scala.reflect.io.File
import scala.sys.process.{Process, ProcessLogger}
import scala.util.control.NonFatal

class RedisProcess(val port: Int) {
  protected var maybeServer: Option[Process] = None
  protected val cmd                               = s"${redisServerCmd} --port $port ${redisServerLogLevel}"
  protected val log                     = Logger(getClass)
  protected val processLogger           = ProcessLogger(line => log.debug(line), line => log.error(line))

  def start(): Unit = {
    log.debug(s"starting $this")
    maybeServer match {
      case None    => maybeServer = Some(Process(cmd).run(processLogger))
      case Some(_) => log.warn(s"$this already started")
    }
  }

  def stop(): Unit = {
    log.debug(s"stopping $this")
    maybeServer match {
      case Some(s) =>
        try {
          val out = new Socket(redisHost, port).getOutputStream
          out.write("SHUTDOWN NOSAVE\n".getBytes)
          out.flush()
          out.close()
        } catch {
          case NonFatal(e) => log.error(s"couldn't stop $this", e)
        } finally {
          s.destroy()
          maybeServer = None
        }
      case None =>
        log.warn(s"$this already stopped")
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

  override protected val cmd = s"${redisServerCmd} $sentinelConfPath --port $port --sentinel $redisServerLogLevel"
}

class SlaveProcess(masterPort: Int, port: Int) extends RedisProcess(port) {
  override protected val cmd = s"$redisServerCmd --port $port --slaveof $redisHost $masterPort $redisServerLogLevel"
}
