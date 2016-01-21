#!/bin/bash
export VERS=0.2.4
export RESULTDIR=/tmp/sparkout
rm -fr ${RESULTDIR}*
export DATADIR=BusDataSample/data
${SPARKBIN}/spark-submit --master spark://Juhas-MBPr.local:7077 --class com.nuvostaq.bigdataspark.BusDataDriver target/scala-2.10/BigDataSpark-assembly-${VERS}.jar ${DATADIR}/BusRoute.16784.\*.gz ${DATADIR}/localweather.16786.\*.gz ${DATADIR}/BusActivity.16785\*.gz ${RESULTDIR}
