package redis

import org.scalactic.Prettifier
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{Matchers, WordSpecLike}

trait TestBase extends WordSpecLike with Matchers with ScalaFutures with Eventually {
  import org.scalatest.time.{Millis, Seconds, Span}
  implicit protected val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(20, Millis))

  def beBetween[T: Ordering](right: (T, T)): Matcher[T] =
    new Matcher[T] {
      def apply(left: T): MatchResult = {
        val (from, to) = right
        val ordering   = implicitly[Ordering[T]]
        val isBetween  = ordering.lteq(from, left) && ordering.gteq(to, left)
        MatchResult(
          isBetween,
          s"$left wasn't in range $right",
          s"$left was in range $right",
          Vector(left, right)
        )
      }
      override def toString: String = "be between " + Prettifier.default(right)
    }
}
