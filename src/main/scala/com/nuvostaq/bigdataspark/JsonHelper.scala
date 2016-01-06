package com.nuvostaq.bigdataspark

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.util.Try

/**
  * Helper functions for deserializing JSON text input
  * Created by juha on 6.1.2016.
  * (c) 2016 Nuvostaq
  */
object JsonHelper {
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
}
