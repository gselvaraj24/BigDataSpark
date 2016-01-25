---
layout: post
title: Extracting and processing data using Spark
summary: This post describes how Spark can be used to extract and process data from the bus timing and weather data sources.
---

This post continues from where we left in the [previous post](../26/Setup.html). Now that Spark is installed, and we
can build and run our initial sample application, it's time to start working on a real data analysis task.

## Enhanced sample program
Let's first check out the next version of the sample.
{% highlight bash %}
cd BigDataSpark
git checkout p2-busdata
{% endhighlight %}

There is a significant number of additions to the initial sample. Let's quickly review them:

* The driver calculates bus journey end points with information about the distance at the beginning and end of the
measurement and the total duration. The distances are needed for checking that the measured time covers the
whole route to weed out partial measurements from the analysis.
* RouteConverter contains a number of utilities, e.g.
	* Converting the query object into a BusRoute
    * Converting GPS locations to a planar metric projection
    * Calculating the overall distance from the route beginning
* The driver also joins the previously extracted information about the weather (the temperature, the amount of rain) with
the endpoint measurement and creates the actual output of the program.
* EndPointCombiner is used to aggregate end point data and find the actual end points
    * The origin is the point with the latest time stamp at the initial location (before which the bus has been idle)
    * The end point is the point with the earliest time stamp at the maximum distance (after which the bus has been idle)
* submitjob.sh bash script for running the program

The additions to the driver are as follows<sup>[1](#footnote1)</sup>:
<script src="https://gist.github.com/nuvostaq/dad3bd7eb5311da1910a.js"></script>

The operation with .combineByKey is worth digesting in more detail. Due to its distributed nature, Spark is sensitive to
how the data is laid out when running in cluster mode, just like the performance of locally run programs depends on
memory access patterns. But since the penalty of data access across a local area network is orders of magnitude higher,
the importance of data access patterns is also much higher. A Spark program will happily churn your data no matter
how it is laid out, but careless planning will certainly yield suboptimal results.

Aggregation operations, such as reductions, are typically where the interesting things happen, but they can also be the
most expensive ones regarding performance. Aggregation is typically done on pair RDDs. The groupByKey is a useful operation
where items with identical keys are grouped. However, it is expensive as every single key-value pair will be shuffled
across the network with identical keys landing on the same reducer. The combineByKey can be used to perform the same
operation, more optimally. With combineByKey, values are merged in each partition, and finally, the values from the
partitions are merged into a single value. An example of using combineByKey is shown [here](http://codingjunkie.net/spark-combine-by-key/).

This example uses the following operations as input to combineByKey (in EndPointCombiner.scala)

* *create* creates an initial EndPointPair from a RoutePoint by using its data both for the start and end
* *mergeValue* compares the input route point to the current endpoint pair and updates the start and end if a better candidate
  is available
* *merger* merges two EndPointPairs to find the best candidate

The final join operation is quite straightforward.

## Running the new version

This version can be built just like before, but execution is simpler with the included script:

{% highlight bash %}
sbt assembly
./submitjob.sh
{% endhighlight %}

We now get two additional results directories

* .epp with the generated endpoint pairs
* .endpweather with the weather data joined

The final output look will like this:

{% highlight bash %}
12,Hallila,16785,3,0,1,1,7,25,7,50,7349.04747803568,9335.924935056653,574.998,0.6,0.0
{% endhighlight %}

Each line contains the following colums as comma separated values:
{% highlight bash %}
"lineNum","originName","epochDay","weekDay","isSummer","isWorkDay","isSchoolDay","scheduledStartHour","scheduledStartMinute","actualStartHour","actualStartMinute","startDist","endDist","duration"
{% endhighlight %}

That was a lot of information crammed into one line, but the CSV format is easy to manipulate in R. And that's exactly what
we are going to do next.

## Data analysis with R

[R](https://www.r-project.org/) is a free software environment for statistical computing and graphics. It is widely used
in data science, and as mentioned in an earlier post, it integrates well with Spark. Here I'm going to show how to use R
as a post processing step for the actual data extraction, but it is possible to run R as part of Spark programs.
R is available for all the desktop operating systems. Please, follow the installation instructions provided on the R
project website.

After the R environment is up and running, you can load the sample script and run it. For now we just calculate the maximum
distance. There really isn't enough data bundled in the sample project for a more detailed analysis. The initial sample
script is:

<script src="https://gist.github.com/nuvostaq/fe825660bf721a706051.js"></script>

We can calculate the average distance of the bus journeys originating from Hallila.

{% highlight R %}
setwd("/path_to_project/BigDataSpark/r-src")
source("bus_analysis.R")
route1Dist
[1] 9335.925
{% endhighlight %}

The [next post](../28/SparkCluster.html) will show how to run Spark on a cluster. With a bit more data,
it is also possible to run a more elaborate statistical analysis with R.

### Footnotes
<div class="footnote">
<a name="footnote1">1</a>: 	The intermediate save actions trigger the evaluation of the transformations and will be
 							removed as redundant later on.
</div>




