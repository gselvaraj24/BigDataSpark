package com.nuvostaq.bigdataspark

import java.util.TimeZone

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.{DateTimeZone, DateTime}

import scala.util.{Failure, Success}

/**
  * Created by juha on 27.12.2015.
  * (c) 2016 Nuvostaq Oy
  */
object BusDataDriver {
  /**
    * Driver main program
    * @param args - routeFileNamePattern weatherFileNamePattern busActivityFileNamePattern resultNameTemplate
    */
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
      JsonHelper.createRoute(record) match {
        case Success(result) => Some(result)
        case Failure(exception) => None
      }
    )
    // Calculate the routes
    val routesRdd = jsonQueries.flatMap {
      case qr: BusRouteQuery => qr.body.map(bd => RouteConverter.convert(bd))
      case _ => None
    }
    routesRdd.persist(StorageLevel.MEMORY_ONLY)
    routesRdd.saveAsTextFile(outputFile+".routes", classOf[GzipCodec])

    // Load weather input data and save it
    val weatherInput =  sc.textFile(weatherFiles)
    println(s"# weather entries = ${weatherInput.count()}")
    val targetPlace = "Tampere Härmälä"
    val weatherRdd = weatherInput.flatMap(queryLine => {
      val queryResult = JsonHelper.createWeather(queryLine)
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
    weatherRdd.persist(StorageLevel.MEMORY_ONLY)
    weatherRdd.saveAsTextFile(outputFile+".weather", classOf[GzipCodec])

    val routes = routesRdd.collect()
    // Load bus activity input
    val activityInput =  sc.textFile(activityFiles)
    println(s"# bus activity entries = ${activityInput.count()}")
    val siriData = activityInput
      .flatMap(record => {
        JsonHelper.createSiri(record) match {
          case  Success(result) =>
            val ts = result.Siri.ServiceDelivery.ResponseTimestamp
            val key = DateTypeConverter.toEpochDay(new DateTime(ts))
            Some((s"$key-siri", result))
          case _ => None
        }
      })

    // Calculate the time-stamped distance for each activity location
    val distsForRoutes = siriData.flatMap {
      case (inputKey, siri: Siri) =>
        Config.followedRoutes.flatMap(rn =>
          routes.filter(rr => rr.name == rn._2) // Only the interesting routes are processed
            .flatMap(rr => {
              try {
                val originName = rn._2.split(" - ").head
                // Get the timed locations
                val locations = ActivityConverter.getLocations(rn._1, originName, siri)
                locations.map(tl => {
                  try {
                    val ts = (tl.timeStamp / 1000).toInt
                    val epochDay = inputKey.split("-").head
                    val startPlace = rn._2.replace(" ", "").split("-")(0)

                    // Create the key: (epochDay, line number, route beginning, bus reference)
                    val key = s"$epochDay-${rn._1}-$startPlace-${tl.ref}"

                    // Convert locations to the distance from the route beginning
                    val dist = RouteConverter.distFromStart((ts, tl.location.Latitude, tl.location.Longitude), rr)

                    // Create the output with line number, route name, bus reference, timestamp and distance
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
    distsForRoutes.persist(StorageLevel.MEMORY_ONLY)
    distsForRoutes.saveAsTextFile(outputFile+".dist", classOf[GzipCodec])

    // Finally, calculate the end point pairs with time and distance info for the start and end
    // using combineByKey aggregation
    val endPointPairs = distsForRoutes
      .combineByKey(EndPointCombiner.create, EndPointCombiner.combiner, EndPointCombiner.merger)
      .filter(_._1.matches("^1[6-9].*")) // Ignore failed keys without epoch day number
      // Process the aggregate and create the result key-value pairs
      .map(epp => {
        val (epochDay, lineNum, originName, lineRef) = epp._1.split("-") match {
          case Array(s1, s2, s3, s4) => (s1, s2, s3, s4)
        }
        val scheduledStartHour = lineRef.startsWith("0") match {
          case true => lineRef.substring(1,2)
          case _ => lineRef.substring(0,2)
        }
        val scheduledStartMinute = lineRef.substring(2,4).startsWith("0") match {
          case true => lineRef.substring(3,4)
          case _ => lineRef.substring(2,4)
        }
        // Determine the date type (weekday, holiday, etc.)
        val startDt = new DateTime(epp._2.startTime, DateTimeZone.forTimeZone(TimeZone.getTimeZone("EET")))
        val actualStartHour = startDt.getHourOfDay
        val actualStartMinute = startDt.getMinuteOfHour
        val duration = (epp._2.endTime - epp._2.startTime).toDouble / 1000
        val dateType = DateTypeConverter.GetType(startDt)

        // Create the new key
        val key = s"$epochDay-$scheduledStartHour"
        // Create the key-value pair
        (key, s"$lineNum,$originName,$epochDay,${PeriodFactory.ConvertToString(dateType)},$scheduledStartHour,$scheduledStartMinute,$actualStartHour,$actualStartMinute,${epp._2.startDist},${epp._2.endDist},$duration")
      })

    // Join the end points with weather data
    val endPointsWithWeather = endPointPairs
      .leftOuterJoin(weatherRdd)
      .map(e => {
        val (_, (busData, weatherData)) = e
        val wd = weatherData match {
          case Some(w)  => s"${w.temp},${w.rain}"
          case None     => "-100,-100"
        }
        s"$busData,$wd"
      })
    // And save the result
    endPointsWithWeather.saveAsTextFile(outputFile+".endpweather")
  }
}
