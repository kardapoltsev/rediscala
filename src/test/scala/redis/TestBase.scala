package redis

import java.nio.file.{Files, Path}

import org.apache.logging.log4j.scala.Logger
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{Matchers, WordSpecLike}

trait TestBase extends WordSpecLike with Matchers with ScalaFutures with Eventually {
  import org.scalatest.time.{Millis, Seconds, Span}
  implicit protected val defaultPatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(100, Millis))

  protected val log = Logger(getClass)

  protected def createTempDirectory(): Path = {
    val dir = Files.createTempDirectory("rediscala-test-dir")
    dir.toFile.deleteOnExit()
    dir
  }

  protected def beBetween[T: Ordering](from: T, to: T): Matcher[T] = {
    val range = s"[$from, $to]"
    new Matcher[T] {
      def apply(left: T): MatchResult = {
        val ordering  = implicitly[Ordering[T]]
        val isBetween = ordering.lteq(from, left) && ordering.gteq(to, left)
        MatchResult(
          isBetween,
          s"$left wasn't in range $range",
          s"$left was in range $range",
          Vector(left, from, to)
        )
      }

      override def toString: String = "be between " + range
    }
  }

}
