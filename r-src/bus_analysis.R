
util.load<-function(fileName, lineNum, originName) {
	dataSet<-read.csv(fileName, head=F, col.names=c("lineNum","originName","epochDay","weekDay","isSummer","isWorkDay","isSchoolDay","scheduledStartHour","scheduledStartMinute","actualStartHour","actualStartMinute","startDist","endDist","duration","temp", "rain"), stringsAsFactors=TRUE)
	rawData<-dataSet[dataSet$lineNum == lineNum & dataSet$originName == originName,]
	dist<-median(rawData$endDist)
	dur<-median(rawData$duration)
	filtered<-rawData[rawData$endDist > 0.99*dist & rawData$duration > 0.5*dur & rawData$startDist < 10 & rawData$temp > -50 & rawData$duration>600,]
	print(summary(filtered))
	return(filtered)
}

util.schoolDayMornings<-function(dataSet) {
	schoolDays<-dataSet[dataSet$isSchoolDay==1,]
	mornings<-schoolDays[schoolDays$scheduledStartHour>=7 & schoolDays$scheduledStartHour<=8,]
	
	return(mornings)
}

util.selectNumeric<-function(dataSet){
	myVars<-c("weekDay","isSchoolDay","scheduledStartHour","temp", "rain")
	return(dataSet[myVars])
}

util.bottomQuantile=function(dataSet){
	q=quantile(dataSet$duration)
	res=dataSet[dataSet$duration<=q[2],]
}
util.topQuantile=function(dataSet){
	q=quantile(dataSet$duration)
	res=dataSet[dataSet$duration>=q[4],]
}
util.midHalf=function(dataSet){
	q=quantile(dataSet$duration)
	res=dataSet[dataSet$duration>q[2] & dataSet$duration<q[4],]
}
util.clustering=function(dataSet){
	n<-util.selectNumeric(dataSet)
	d<-scale(n)
	cntr=attr(d,"scaled:center")
	scl=attr(d,"scaled:scale")
	fit=pamk(d)
	m=fit$pamobject$medoids
	for(i in 1:fit$nc){
		print(m[i,]*scl+cntr)
	}
	return(fit)
}
util.correlation=function(dataSet){
	n<-util.selectNumeric(dataSet)
	return(cov(n))
}

util.compareStart=function(data1, data2){
	r1=data1$scheduledStartHour
	l1=length(r1)
	r2=data2$scheduledStartHour
	l2=length(r2)
	sc=1/(l1+l2)

	d1=density(r1)
	d2=density(r2)
	plot(c(5,22),c(0,0.10),type="n",xlab="Start hour",ylab="")
	lines(d1$x,sc*d1$y*l1)
	lines(d2$x,sc*d2$y*l2,col="red")
}

util.compareTemp=function(data1, data2){
	r1=data1$temp
	l1=length(r1)
	r2=data2$temp
	l2=length(r2)
	sc=1/(l1+l2)

	d1=density(r1)
	d2=density(r2)
	plot(c(-25,10),c(0,0.10),type="n",xlab="temperature C",ylab="")
	lines(d1$x,sc*d1$y*l1)
	lines(d2$x,sc*d2$y*l2,col="red")
}

util.compareDur=function(data1, data2,data3){
	r1=data1$duration/60
	l1=length(r1)
	r2=data2$duration/60
	l2=length(r2)
	r3=data3$duration/60
	l3=length(r3)
	
	d1=density(r1)
	d2=density(r2)
    d3=density(r3)

	sc=1/(l1+l2+l3)

	plot(c(10,40),c(0,0.15),type="n",xlab="minutes",ylab="")
	lines(d1$x,sc*d1$y*l1)
	lines(d2$x,sc*d2$y*l2,col="red")
	lines(d3$x,sc*d3$y*l3,col="blue")
}

route1<-util.load("/tmp/SparkResults/res1701.csv", 12, "Hallila")
t1=util.topQuantile(route1)
b1=util.bottomQuantile(route1)
m1=util.midHalf(route1)

util.compareStart(t1,b1)
util.compareDur(t1,b1,m1)
dm=density(m1$scheduledStartHour)
# Clustering experiments

route2<-util.load("/tmp/SparkResults/res1701.csv", 32, "Keilakuja")
t2=util.topQuantile(route2)
b2=util.bottomQuantile(route2)

tn2<-util.selectNumeric(t2)
bn2<-util.selectNumeric(b2)
dt=density(tn2$scheduledStartHour)
db=density(bn2$scheduledStartHour)

plot(c(5,22),c(0,0.5),type="n",xlab="h")

lines(dt)
lines(db,col="red")
td2<-scale(tn2)
c2=attr(td2,"scaled:center")
sc2=attr(d,"scaled:scale")

fit<-pamk(d)
fit$pamobject$medoids

m=fit$pamobject$medoids
m[1,]*sc2+c2


summary(route1$endDist)
route1Dist<-max(route1$endDist)
route1Dist<-median(route1$endDist)
route1FilteredEnd<-route1[route1$endDist>0.98*route1Dist,]
route1FilteredDur<-route1FilteredEnd[route1FilteredEnd$duration>300,]
route1Filtered<-route1FilteredDur[route1FilteredDur$startDist<50 & route1FilteredDur$temp>-50,]
route1Filtered$h<-route1Filtered$scheduledStartHour
route1Filtered$hm<-route1Filtered$scheduledStartHour+route1Filtered$scheduledStartMinute/60.0
schoolDays<-route1Filtered[route1Filtered$isSchoolDay==1,]
thuMornings<-schoolDays[schoolDays$h>=7&schoolDays$h<=8&schoolDays$weekDay==4,]
slowMornings<-thuMornings[thuMornings$duration>1899,]
fastMornings<-thuMornings[thuMornings$duration<1650,]

mornings<-schoolDays[schoolDays$h>=7&schoolDays$h<=8,]
afternoons<-schoolDays[schoolDays$h>=15&schoolDays$h<=17,]


plusTempDays <- tempDays[tempDays $temp>0,]
minusTempDays <- tempDays[tempDays $temp<=0,]
coldDays <- tempDays[tempDays $temp<=-10,]

dryWeather<-workDays[workDays $rain==0,]
rainyWeather<-workDays[workDays $rain>0.1,]

pairs(~duration+h, coldDays)
pairs(~duration+weekDay, mornings)

route2<-dataset1[dataset1$originName=="KeskustoriP",]
route2Dist<-median(route2$endDist)
route2FilteredEnd<-route2[route2$endDist>0.98*route2Dist,]
route2FilteredDur<-route2FilteredEnd[route2FilteredEnd$duration>300,]
route2Filtered<-route2FilteredDur[route2FilteredDur$startDist<50 & route2FilteredDur$temp>-50,]
schoolDays<-route2Filtered[route2Filtered$isSchoolDay==1,]
afternoons<-schoolDays[schoolDays$scheduledStartHour>=15&schoolDays$scheduledStartHour<=16,]
pairs(~duration+weekDay, afternoons)
slowAfternoons<-afternoons[afternoons$duration>1808,]
fastAfternoons<-afternoons[afternoons $duration<1613,]
