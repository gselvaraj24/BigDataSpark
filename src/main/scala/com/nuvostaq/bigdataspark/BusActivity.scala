package com.nuvostaq.bigdataspark

/**
  * Created by juha on 3.1.2016.
  * (c) 2016 Nuvostaq
  */
case class VehicleLocation(Longitude: Double, Latitude: Double)
case class VehicleRef(value: String)
case class DestinationName(value: String)
case class OriginName(value: String)
case class LineRef(value: String)
case class FramedVehicleJourneyRef(DatedVehicleJourneyRef:String)
case class MonitoredVehicleJourney(LineRef: LineRef, OriginName:OriginName, DestinationName:DestinationName, VehicleRef: VehicleRef, VehicleLocation:VehicleLocation, Bearing: Int, Delay: String, FramedVehicleJourneyRef: FramedVehicleJourneyRef)
case class VehicleActivity(MonitoredVehicleJourney: MonitoredVehicleJourney, RecordedAtTime: Long)
case class VehicleMonitoringDelivery(VehicleActivity: Seq[VehicleActivity])
case class ServiceDelivery(ResponseTimestamp: Long, VehicleMonitoringDelivery: Seq[VehicleMonitoringDelivery])
case class SiriData(ServiceDelivery: ServiceDelivery, version: String)
case class Siri(Siri: SiriData)

// This is the output type; list of time-stamped vehicles locations including the journey reference,
// which is used to identify the actual bus
case class TimedLocation(ref:String,location: VehicleLocation, timeStamp: Long)

/**
  * Converts the bus activity request object (Siri) into a sequence of timed locations
  */
object ActivityConverter {
  def getLocations(lineRef: String, originName: String, siri: Seq[Siri]): Seq[TimedLocation] = {
    siri.flatMap(s => getLocations(lineRef, originName, s))
  }

  def getLocations(lineRef: String, originName: String, siri: Siri): Seq[TimedLocation] = {
    val activities = siri.Siri.ServiceDelivery.VehicleMonitoringDelivery.flatMap(vd => vd.VehicleActivity)
    val fOut = activities.filter(p => {
      try {
        p.MonitoredVehicleJourney.LineRef.value == lineRef && p.MonitoredVehicleJourney.OriginName.value == originName
      } catch {
        case e: Exception => false
      }
    })
    fOut.map(a => new TimedLocation(a.MonitoredVehicleJourney.FramedVehicleJourneyRef.DatedVehicleJourneyRef, a.MonitoredVehicleJourney.VehicleLocation, a.RecordedAtTime))
  }
}
