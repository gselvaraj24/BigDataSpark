package com.nuvostaq.bigdataspark

/**
  * BusData and route case classes
  * Created by juha on 2.1.2016.
  * (c) 2016 Nuvostaq Oy
  */
case class RouteBody(name: String, geographicCoordinateProjection: String)
case class BusRouteQuery(status: String, body: Seq[RouteBody])
case class BusRoute(name : String = "", gpsCoords : Seq[(Int, Double, Double)], metricCoords : Seq[(Int, Double, Double)], cumDistance : Seq[Double])
case class RoutePoint(line:String, route: String, start: String, time:Long, dist: Double)
case class EndPointPair(startTime:Long, startDist: Double, endTime:Long, endDist: Double)
