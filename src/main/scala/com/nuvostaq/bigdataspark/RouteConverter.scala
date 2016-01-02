package com.nuvostaq.bigdataspark

/**
  * Created by juha on 2.1.2016.
  * (c) 2016 Nuvostaq Oy
  */
object  RouteConverter {
  val coordinateScale = 100000.0

  /**
    * Converts a bus route query result into a BusRoute object
    * @param queryBody - query ersult
    * @return BusData object
    */
  def convert(queryBody: RouteBody): BusRoute = {
    val name = queryBody.name
    val routes = queryBody.geographicCoordinateProjection
      .split(":")
      .map(rawPt => rawPt.split(","))
      .map(split => (split(0).toDouble / coordinateScale, split(1).toDouble / coordinateScale))


    val len = routes.length
    val coordinateSeq = new Array[(Int, Double, Double)](len)

    // Unpack the route as per http://wiki.itsfactory.fi/index.php/Journeys_API#Routes_2
    coordinateSeq(0) = (0, routes(0)._1, routes(0)._2)
    for (i <- 1 to len - 1) {
      coordinateSeq(i) = (i, coordinateSeq(i - 1)._2 - routes(i)._1, coordinateSeq(i - 1)._3 - routes(i)._2)
    }

    // Create the metric coordinates
    val metricCoordinates = coordinateSeq.map(RouteConverter.gpsToMetric)

    // Calculate the cumulative distance
    val distances = new Array[Double](len)
    distances(0) = 0.0
    for (i <- 1 to len - 1) {
      distances(i) = distances(i - 1) + RouteConverter.dist(metricCoordinates(i), metricCoordinates(i - 1))
    }

    new BusRoute(name, coordinateSeq, metricCoordinates, distances)
  }

  /**
    * Calculates the distance of a bus location from the route start point.
    * The position is projected to the closest point onto the route.
    * @param gpsLoc - GPS location
    * @param route  - BusRoute
    * @return       - Distance in meters
    */
  def distFromStart(gpsLoc: (Int, Double, Double), route: BusRoute): Double = {
    val metricPt = gpsToMetric(gpsLoc)
    // First find the closest point in the route
    def returnMin(x: (Int, Double), y: (Int, Double)): (Int, Double) = {
      if (x._2 < y._2) return x
      y
    }

    // Find the closest point on the route
    val closestPoint = route.metricCoords.foldLeft((-1, 10000000.0)) {
      (a, p) =>
        val d = dist(p, metricPt)
        returnMin(a, (p._1, d))
    }

    // Second find the closest segment in the route
    case class Point(id: Int, x: Double, y: Double)
    case class Segment(p0: Point, p1: Point, offset: Double, dist: Double)


    val len = route.metricCoords.length
    val segments = new Array[Segment](len - 1)
    for (i <- 0 to len - 2) {
      val pt0 = route.metricCoords(i)
      val pt1 = route.metricCoords(i + 1)
      val v0 = vecSub(pt0, metricPt)
      val v1 = vecSub(pt0, pt1)
      val v1d = Math.sqrt(v1._1 * v1._1 + v1._2 * v1._2)
      val proj = project(v0, v1)
      var d = 10000000.0
      if (proj > 0.0 && proj < v1d) {
        val v0d2 = v0._1 * v0._1 + v0._2 * v0._2
        d = Math.sqrt(v0d2 - proj * proj)
      }
      segments(i) = new Segment(new Point(pt0._1, pt0._2, pt0._3), new Point(pt1._1, pt1._2, pt1._3), proj, d)
    }
    val tmpPt = new Point(0, 0.0, 0.0)

    def returnMinSeg(s0: Segment, s1: Segment): Segment = {
      if (s1.dist < s0.dist) return s1
      s0
    }

    val closestSegment = segments.foldLeft(new Segment(tmpPt, tmpPt, 0, 100000.0)) {
      (s0, s1) =>
        returnMinSeg(s0, s1)
    }

    if (closestPoint._2 < closestSegment.dist) {
      return route.cumDistance(closestPoint._1)
    }
    route.cumDistance(closestSegment.p0.id) + closestSegment.offset
  }

  /**
    * Euclidean distance between two points
    * @param pt1 - Point 1
    * @param pt2 - Point 2
    * @return    - Distance
    */
  def dist(pt1: (Int, Double, Double), pt2: (Int, Double, Double)): Double = {
    val d1 = pt1._2 - pt2._2
    val d2 = pt1._3 - pt2._3

    Math.sqrt(d1 * d1 + d2 * d2)
  }

  /**
    * Convert a GPS location (latitude and longitude in degrees) to the TM35FIN metric projection system
    * http://www.maanmittauslaitos.fi/kartat/koordinaatit/tasokoordinaatistot/etrs-tm35fin
    * The conversion is implemented on the basis of: http://docs.jhs-suositukset.fi/jhs-suositukset/JHS154_liite1/JHS154_liite1.html
    * @param gpsPoint - GPS location (index,latitude_degrees,longitude_degrees)
    * @return
    */
  def gpsToMetric(gpsPoint: (Int, Double, Double)): (Int, Double, Double) = {
    val latdeg = gpsPoint._2
    val longdeg = gpsPoint._3

    val ff = 1.0 / 298.257222101
    val L0 = 27.0 / 180.0 * Math.PI
    val k0 = 0.9996
    val E0 = 500000.0
    val A = 6378137.0
    var e = Math.sqrt(2 * ff - ff * ff)
    val n = ff / (2 - ff)
    val h11 = n / 2 - 2.0 / 3.0 * n * n + 5.0 / 16.0 * (n * n * n) + 41.0 / 180.0 * (n * n * n * n)
    val h21 = 13.0 / 48.0 * n * n - 3.0 / 5.0 * (n * n * n) + 557.0 / 1440.0 * (n * n * n * n)
    val h31 = 61.0 / 240.0 * (n * n * n) - 103.0 / 140.0 * (n * n * n * n)
    val h41 = 49561.0 / 161280.0 * (n * n * n * n)

    val T = latdeg / 180 * Math.PI
    val L = longdeg / 180 * Math.PI
    val Q1 = asinh(Math.tan(T))
    val Q2 = atanh(e * Math.sin(T))
    val Q = Q1 - e * Q2
    val l = L - L0
    val bb = Math.atan(sinh(Q))

    val gg = atanh(Math.cos(bb) * Math.sin(l))
    val zz = Math.asin(Math.sin(bb) * cosh(gg))

    val z1 = h11 * Math.sin(2 * zz) * cosh(2 * gg)
    val z2 = h21 * Math.sin(4 * zz) * cosh(4 * gg)
    val z3 = h31 * Math.sin(6 * zz) * cosh(6 * gg)
    val z4 = h41 * Math.sin(8 * zz) * cosh(8 * gg)
    val g1 = h11 * Math.cos(2 * zz) * sinh(2 * gg)
    val g2 = h21 * Math.cos(4 * zz) * sinh(4 * gg)
    val g3 = h31 * Math.cos(6 * zz) * sinh(6 * gg)
    val g4 = h41 * Math.cos(8 * zz) * sinh(8 * gg)

    val A1 = A / (1 + n) * (1 + n * n / 4 + n * n * n * n / 64)
    e = Math.sqrt(2 * ff - ff * ff)
    val z = zz + z1 + z2 + z3 + z4
    val g = gg + g1 + g2 + g3 + g4

    val latitude = Math.round(A1 * z * k0)
    val longitude = Math.round(A1 * g * k0 + E0)

    (gpsPoint._1, latitude, longitude)
  }

  // Vector and trigonometric operations

  private def project(v0: (Double, Double), v1: (Double, Double)): Double = {
    val v1Len = Math.sqrt(v1._1 * v1._1 + v1._2 * v1._2)

    v0._1 * v1._1 / v1Len + v0._2 * v1._2 / v1Len
  }

  private def vecSub(p0: (Int, Double, Double), p1: (Int, Double, Double)): (Double, Double) = {
    (p1._2 - p0._2, p1._3 - p0._3)
  }

  private def atanh(x: Double): Double = {
    0.5 * Math.log((1.0 + x) / (1.0 - x))
  }

  private def asinh(x: Double): Double = {
    Math.log(x + Math.sqrt(x * x + 1))
  }

  private def sinh(x: Double): Double = {
    (Math.exp(x) - Math.exp(-x)) / 2
  }

  private def cosh(x: Double): Double = {
    (Math.exp(x) + Math.exp(-x)) / 2
  }
}
