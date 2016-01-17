dataset1<-read.csv("/tmp/result.csv", head=F, col.names=c("lineNum","originName","epochDay","weekDay","isSummer","isWorkDay","isSchoolDay","scheduledStartHour","scheduledStartMinute","actualStartHour","actualStartMinute","startDist","endDist","duration","temp", "rain"), stringsAsFactors=FALSE)
route1<-dataset1[dataset1$originName=="Hallila",]
summary(route1$endDist)
route1Dist<-max(route1$endDist)
route1Dist<-median(route1$endDist)
route1FilteredEnd<-route1[route1$endDist>0.98*route1Dist,]
route1FilteredDur<-route1FilteredEnd[route1FilteredEnd$duration>300,]
route1Filtered<-route1FilteredDur[route1FilteredDur$startDist<50,]
route1Filtered$h<-route1Filtered$scheduledStartHour
route1Filtered$hm<-route1Filtered$scheduledStartHour+route1Filtered$scheduledStartMinute/60.0
pairs(~duration+h, route1Filtered)