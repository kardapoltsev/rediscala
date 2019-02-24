package redis.commands

import redis._
import redis.api.geo.DistUnits._


class GeoSpec extends RedisStandaloneServer {

  val testKey = "Sicily"

  def addPlaces() = {
    redis.geoAdd(testKey, 13.361389, 38.115556, "Palermo").futureValue
    redis.geoAdd(testKey, 15.087269, 37.502669, "Catania").futureValue
    redis.geoAdd(testKey, 13.583333, 37.316667, "Agrigento").futureValue

  }

  "Geo commands " should {

    "GEOADD add key member" in {
      val res = redis.geoAdd(testKey, 23.361389, 48.115556, "SomePlace").futureValue
      res shouldEqual 1
    }

    "GEORADIUS" in {
      addPlaces()
      redis.geoRadius(testKey, 15, 37, 200, Kilometer).futureValue shouldEqual Vector("Agrigento", "Catania")
    }

    "GEORADIUS By Member" in {
      addPlaces()
      redis.geoRadiusByMember(testKey, "Catania", 500, Kilometer).futureValue shouldEqual Vector(
        "Agrigento",
        "Palermo",
        "Catania")
    }

    "GEORADIUS By Member with opt" in {
      addPlaces()
      val res = redis.geoRadiusByMemberWithOpt(testKey, "Agrigento", 100, Kilometer).futureValue
      res shouldEqual Vector("Agrigento", "0.0000", "Palermo", "89.8694")
    }

    "GEODIST with default unit" in {
      addPlaces()
      val res = redis.geoDist(testKey, "Palermo", "Catania").futureValue
      res shouldEqual 203017.1901
    }

    "GEODIST in km" in {
      addPlaces()
      val res = redis.geoDist(testKey, "Palermo", "Catania", Kilometer).futureValue
      res shouldEqual 203.0172
    }

    "GEODIST in mile" in {
      addPlaces()
      val res = redis.geoDist(testKey, "Palermo", "Catania", Mile).futureValue
      res shouldEqual 126.1493
    }

    "GEODIST in feet" in {
      addPlaces()
      val res = redis.geoDist(testKey, "Palermo", "Catania", Feet).futureValue
      res shouldEqual 666066.8965
    }

    "GEOHASH " in {
      addPlaces()
      val res = redis.geoHash(testKey, "Palermo", "Catania").futureValue
      res shouldEqual Vector("sfdtv6s9ew0", "sf7h526gsz0")
    }

    "GEOPOS " in {
      addPlaces()
      val res = redis.geoPos(testKey, "Palermo", "Catania").futureValue
      res should not be empty
    }
  }
}
