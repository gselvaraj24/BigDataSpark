dataset1<-read.csv("/tmp/result.csv", head=F, col.names=c("lineNum","originName","epochDay","weekDay","isSummer","isWorkDay","isSchoolDay","scheduledStartHour","scheduledStartMinute","actualStartHour","actualStartMinute","startDist","endDist","duration","temp", "rain"), stringsAsFactors=FALSE)
route1<-dataset1[dataset1$originName=="Hallila",]
summary(route1$endDist)
route1Dist<-max(route1$endDist)
