package com.nuvostaq.bigdataspark

import java.util.TimeZone

import org.joda.time.{DateTimeZone, DateTime}

/**
  * Created by juha on 2.1.2016.
  * (c) 2016 Nuvostaq Oy
  */
case class WeatherData(time:DateTime, temp:Double, rain: Double)
case class TempData(time: DateTime, value:Double)
case class RainData(time: DateTime, value:Double)
case class WeatherDataRaw(time:String, value:Double)
case class WeatherDataSet(place:String, dataMap:Map[String,Seq[WeatherDataRaw]])
case class WeatherDataQuery(seq:Seq[WeatherDataSet])

class WeatherDataConverter {
  var weatherDataSeq: Seq[(String, WeatherData)] = List(("empty", null))

  def createDataSet(dataMap: Map[String, Seq[WeatherDataRaw]]) = {
    val tempData = dataMap.getOrElse("t2m", List())
    val rainData = dataMap.getOrElse("r_1h", List())
    val tempDataSet = tempData.map(rd => new TempData(convertToDateTime(rd.time), rd.value))
    val rainDataSet = rainData.map(rd => new RainData(convertToDateTime(rd.time), rd.value))

    val rainSet = rainDataSet.groupBy(d => createKey(d.time))

    weatherDataSeq = tempDataSet.map(td => {
      val key = createKey(td.time)
      val rain = rainSet.contains(key) match {
        case true => rainSet(key).head.value
        case _ => 0.0
      }
      (key, new WeatherData(td.time, temp = td.value, rain = rain))
    })
  }

  def getTempAndRain(time: String): Option[WeatherData] = {
    val requestTime = convertToDateTime(time)
    val key = createKey(requestTime)

    val requestMinutes = requestTime.minuteOfHour.get

    val foundByKey = weatherDataSeq.filter(w => w._1 == key)
    if (foundByKey.isEmpty) return None

    val result = foundByKey.map(w => (Math.abs(w._2.time.minuteOfHour.get - requestMinutes), w._2))
      .minBy(w => w._1)
    Some(result._2)
  }

  private def createKey(time: DateTime) = {
    val adjustedTime = time.minusHours(2)
    val times = adjustedTime.getMillis / 1000
    val epochDay = times / (24L * 3600)

    s"$epochDay-${time.hourOfDay.get}"
  }

  private def convertToDateTime(date: String) = {
    val t = DateTime.parse(date)
    new DateTime(t, DateTimeZone.forTimeZone(TimeZone.getTimeZone("EET")))
  }
}