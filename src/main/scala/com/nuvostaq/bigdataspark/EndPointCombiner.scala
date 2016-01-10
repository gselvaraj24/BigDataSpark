package com.nuvostaq.bigdataspark

/**
  * Created by juha on 6.1.2016.
  * (c) 2016 Nuvostaq
  */
object EndPointCombiner {
  /**
    * Creates an endpoint combiner
    * @param pt - route point
    * @return   - EndPointPair
    */
  def createCombiner(pt: RoutePoint) : EndPointPair = {
    if (pt == null)
      null
    else
      new EndPointPair(pt.time, pt.dist, pt.time, pt.dist)
  }

  /**
    * Combines a new route point with the current aggregate by selecting
    *  - the latest start time at the bus's start location (closest to 0)
    *  - the earliest end time at the bus's final location (maximum distance)
    * @param collector - current end point pair
    * @param pt        - new route point
    * @return          - new collector with start and end point candidates
    */
  def mergeValue(collector: EndPointPair, pt: RoutePoint) : EndPointPair = {
    if (collector == null){
      return createCombiner(pt)
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

  /**
    * Merges the results from two collectors and returns a new EndPointPair by selecting
    *  - the latest start time at the bus's start location (closest to 0)
    *  - the earliest end time at the bus's final location (maximum distance)
    * @param collector1 - first collector
    * @param collector2 - second collector
    * @return
    */
  def merger(collector1: EndPointPair, collector2: EndPointPair) : EndPointPair = {
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
}
