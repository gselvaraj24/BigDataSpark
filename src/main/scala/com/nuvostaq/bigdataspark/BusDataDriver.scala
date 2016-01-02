package com.nuvostaq.bigdataspark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by juha on 27.12.2015.
  * (c) 2016 Nuvostaq Oy
  */
object BusDataDriver {
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
    routeInput.saveAsTextFile(outputFile+".routes")

    // Load weather input data and save it
    val weatherInput =  sc.textFile(weatherFiles)
    println(s"# weather entries = ${weatherInput.count()}")
    weatherInput.saveAsTextFile(outputFile+".weather")

    // Load bus activity input data and save it
    val activityInput =  sc.textFile(activityFiles)
    println(s"# bus activity entries = ${activityInput.count()}")
    activityInput.saveAsTextFile(outputFile+".buses")
  }
}
