package redis

case class RedisVersion(major: Int, minor: Int, patch: Int) extends Ordered[RedisVersion] {

  import scala.math.Ordered.orderingToOrdered

  override def compare(that: RedisVersion): Int =
    (this.major, this.minor, this.patch)
      .compare((that.major, that.minor, that.patch))
}
