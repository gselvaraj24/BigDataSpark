package com.nuvostaq.bigdataspark

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}

/**
  * Created by juha on 27.12.2015.
  * (c) 2016 Nuvostaq Oy
  */
object BusDataDriver {
  val followedRoutes = List(
    ("12", "Hallila - Keskustori P"),
    ("12", "Keskustori P - Hallila"),
    ("32", "Keilakuja - Hatanpään sairaala"),
    ("32", "Hatanpään sairaala - Keilakuja")
  )
  val SecondsPerHour: Long = 60 * 60
  val SecondsPerDay: Long = 24 * SecondsPerHour
  val partitionCount = 10

  def createRoute(str:String) : Try[BusRouteQuery] = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    Try(m.readValue(str, classOf[BusRouteQuery]))
  }

  def createSiri(str:String) : Try[Siri] = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    Try(m.readValue(str, classOf[Siri]))
  }

  def createWeather(str:String) : Try[WeatherDataQuery] = {
    val queryLine = "{\"seq\":"+str+"}"
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    Try(mapper.readValue(queryLine, classOf[WeatherDataQuery]))
  }

  def createEndpointCombiner(pt: RoutePoint) : EndPointPair = {
    if (pt == null)
      null
    else
      new EndPointPair(pt.time, pt.dist, pt.time, pt.dist)
  }

  def endpointCombiner (collector: EndPointPair, pt: RoutePoint) : EndPointPair = {
    if (collector == null){
      return createEndpointCombiner(pt)
    }
    if (pt == null) return collector

    var startTime = collector.startTime
    var startDist = collector.startDist
    var endTime = collector.endTime
    var endDist = collector.endDist

    if (pt.dist < startDist || (pt.dist == startDist && pt.time > startTime)){
      startTime = pt.time
      startDist = pt.dist
    }
    if (pt.dist > endDist || (pt.dist == endDist && pt.time < endTime)){
      endTime = pt.time
      endDist = pt.dist
    }
    new EndPointPair(startTime, startDist, endTime, endDist)
  }
  def endpointMerger (collector1: EndPointPair, collector2: EndPointPair) : EndPointPair = {
    if (collector1 == null) return collector2
    if (collector2 == null) return collector1

    var startTime = collector1.startTime
    var startDist = collector1.startDist
    var endTime = collector1.endTime
    var endDist = collector1.endDist
    if (collector2.startDist < startDist || (collector2.startDist == startDist && collector2.startTime > startTime)){
      startTime = collector2.startTime
      startDist = collector2.startDist
    }
    if (collector2.endDist > endDist || (collector2.endDist == endDist && collector2.endTime < endTime)){
      endTime = collector2.endTime
      endDist = collector2.endDist
    }
    new EndPointPair(startTime, startDist, endTime, endDist)
  }


  def main(args: Array[String]) {
    // Input parameters
    val routeFiles = args(0)
    val weatherFiles = args(1)
    val activityFiles = args(2)
    val outputFile = args.last

    // Initialize the Spark context
    val conf = new SparkConf().setAppName(BusDataDriver.getClass.getName)
    val sc = new SparkContext(conf)

    // Load route input data and save it
    val routeInput =  sc.textFile(routeFiles)
    println(s"# route entries = ${routeInput.count()}")
    // Read and process json
    val jsonQueries = routeInput.flatMap(record =>
      createRoute(record) match {
        case Success(result) => Some(result)
        case Failure(exception) => None
      }
    )
    // Calculate the routes
    val routesRdd = jsonQueries.flatMap {
      case qr: BusRouteQuery => qr.body.map(bd => RouteConverter.convert(bd))
      case _ => None
    }
    routesRdd.saveAsTextFile(outputFile+".routes", classOf[GzipCodec])

    // Load weather input data and save it
    val weatherInput =  sc.textFile(weatherFiles)
    println(s"# weather entries = ${weatherInput.count()}")
    val targetPlace = "Tampere Härmälä"
    val weatherRdd = weatherInput.flatMap(queryLine => {
      val queryResult = createWeather(queryLine)
      queryResult match {
        case Success(result) => Some(result)
        case Failure(exception) => None
      }
    }).flatMap(wq => {
      val data = wq.seq.filter(s => s.place == targetPlace)
      if (data.isEmpty){
        None
      } else {
        val weatherDataConverter = new WeatherDataConverter()
        weatherDataConverter.createDataSet(data.head.dataMap)
        Some(weatherDataConverter.weatherDataSeq)
      }
    }).flatMap(w => w)
    weatherRdd.saveAsTextFile(outputFile+".weather", classOf[GzipCodec])

    val routes = routesRdd.collect()
    // Load bus activity input
    val activityInput =  sc.textFile(activityFiles)
    println(s"# bus activity entries = ${activityInput.count()}")
    val siriData = activityInput
      .flatMap(record => {
        createSiri(record) match {
          case  Success(result) => {
            val ts = result.Siri.ServiceDelivery.ResponseTimestamp
            val key = DateTypeConverter.toEpochDay(new DateTime(ts))
            Some((s"$key-siri", result))
          }
          case _ => None
        }
      })

    // Calculate the time-stamped distance for each activity location
    val distsForRoutes = siriData.flatMap {
      case (inputKey, siri: Siri) =>
        followedRoutes.flatMap(rn =>
          routes.filter(rr => rr.name == rn._2)
            .flatMap(rr => {
              try {
                val originName = rn._2.split(" - ").head
                val locations = ActivityConverter.getLocations(rn._1, originName, siri)
                locations.map(tl => {
                  try {
                    val ts = (tl.timeStamp / 1000).toInt
                    val epochDay = inputKey.split("-").head
                    val startPlace = rn._2.replace(" ", "").split("-")(0)
                    val key = s"$epochDay-${rn._1}-$startPlace-${tl.ref}"
                    val dist = RouteConverter.distFromStart((ts, tl.location.Latitude, tl.location.Longitude), rr)
                    (key, new RoutePoint(rn._1, rn._2, tl.ref, tl.timeStamp, dist))
                  } catch {
                    case e: Exception => ("na-process locations", new RoutePoint("", "", "", 0, 0.0))
                  }
                })
              } catch {
                case e: Exception => List(("na-process activities", new RoutePoint(rn._1, rn._2, "", 0, 0.0)))
              }
            })
        )
      case _ => List(("na-process siri", new RoutePoint("", "", "", 0, 0.0)))
    }
    distsForRoutes.saveAsTextFile(outputFile+".dist", classOf[GzipCodec])
  }
}
