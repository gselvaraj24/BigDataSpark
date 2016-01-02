#!/bin/bash
export VERS=0.2.0
export DATADIR=src/test/resources
export RESULTDIR=/tmp/sparkout
rm -fr ${RESULTDIR}*
${SPARKBIN}/spark-submit --class com.nuvostaq.bigdataspark.BusDataDriver target/scala-2.10/BigDataSpark-assembly-${VERS}.jar ${DATADIR}/BusRoute.16784.21.5.gz ${DATADIR}/localweather.16786.4.0.gz ${DATADIR}/BusActivity.16785.8.0.gz ${RESULTDIR}
