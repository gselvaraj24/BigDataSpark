package com.nuvostaq.bigdataspark

/**
  * Created by juha on 3.1.2016.
  * (c) 2016 Nuvostaq
  */

import java.util.GregorianCalendar

import org.joda.time.{DateTimeConstants, DateTime}
import org.joda.time.format.DateTimeFormat

case class DateType(weekDay: Int, isSummer: Boolean, isWorkDay: Boolean, isSchoolDay: Boolean)

// The output type representing a time period
case class TimePeriod(start: DateTime, end: DateTime)

/**
  * Used to create time periods as DateTime objects from strings
  */
object PeriodFactory {
  /**
    * Creates a new time periods
    * @param startStr - start time as string
    * @param endStr   - end time as string
    * @return         - TimePeriod object
    */
  def Create(startStr: String, endStr: String): TimePeriod = {
    new TimePeriod(ConvertToDateTime(startStr), ConvertToDateTime(endStr))
  }

  def ConvertToDateTime(date: String) = {
    val fmt = DateTimeFormat.forPattern("dd.MM.yyyy")
    DateTime.parse(date, fmt)
  }

  def IsWithin(time: String, period: TimePeriod) : Boolean = {
    IsWithin(ConvertToDateTime(time), period)
  }

  def IsOutside(time: DateTime, period: TimePeriod) = {
    period.start.compareTo(time) > 0 || period.end.compareTo(time) < 0
  }

  def IsWithin(time: DateTime, period: TimePeriod) = {
    period.start.compareTo(time) <= 0 && period.end.compareTo(time) >= 0
  }

  def IsOutside(time: String, period: TimePeriod) : Boolean = {
    IsOutside(ConvertToDateTime(time), period)
  }

  def ConvertToString(dateType: DateType) = {
    var isSummer = 0
    if (dateType.isSummer) isSummer = 1
    var isWorkDay = 0
    if (dateType.isWorkDay) isWorkDay = 1
    var isSchoolDay = 0
    if (dateType.isSchoolDay) isSchoolDay = 1

    s"${dateType.weekDay},$isSummer,$isWorkDay,$isSchoolDay"
  }

}

/**
  * Utility for date and period operations
  */
object DateTypeConverter {
  val calendar = new GregorianCalendar()

  val schoolHolidays = List(
    ("19.12.2015","7.1.2016"),
    ("29.2.2016","4.3.2016"),
    ("25.3.2016","28.3.2016"),
    ("5.6.2016","10.8.2016"),
    ("17.10.2016","21.10.2016"),
    ("23.12.2016","8.1.2017"),
    ("27.2.2017","3.3.2017")
  )
  val publicHolidays = List(
    ("24.12.2015","26.12.2015"),
    ("1.1.2016","1.1.2016"),
    ("6.1.2016","6.1.2016"),
    ("25.3.2016","28.3.2016"),
    ("25.3.2016","28.3.2016"),
    ("5.5.2016","5.5.2016"),
    ("5.5.2016","5.5.2016"),
    ("24.6.2016","26.6.2016"),
    ("6.12.2016","6.12.2016")
  )
  val summerPeriods = List(
    ("5.6.2016","10.8.2016")
  )

  def IsWorkingDay(date: DateTime) = {
    date.dayOfWeek.get >= DateTimeConstants.MONDAY && date.dayOfWeek.get <= DateTimeConstants.FRIDAY
  }

  /**
    * Determines the date type: working day, holiday, summer, weekday
    * @param date - input date
    * @return     - date type
    */
  def GetType(date: DateTime) = {
    val weekDay = date.dayOfWeek().get
    val isWorkingWeekDay = IsWorkingDay(date)

    val isSchoolDay = schoolHolidays.forall(periodStrPair =>
      isWorkingWeekDay && PeriodFactory.IsOutside(date, PeriodFactory.Create(periodStrPair._1, periodStrPair._2)))

    val isWorkDay = publicHolidays.forall(periodStrPair =>
      isWorkingWeekDay && PeriodFactory.IsOutside(date, PeriodFactory.Create(periodStrPair._1, periodStrPair._2)))

    val isSummerPeriod = summerPeriods.forall(periodStrPair =>
      PeriodFactory.IsWithin(date, PeriodFactory.Create(periodStrPair._1, periodStrPair._2)))

    new DateType(weekDay = weekDay, isSummer = isSummerPeriod, isWorkDay = isWorkDay, isSchoolDay = isSchoolDay)
  }

  /**
    * Calculates the number of days from the Unix epoch
    * @param date - date
    * @return     - unix epoch day
    */
  def toEpochDay(date:DateTime) : Long = {
    toEpochDay(date.year().get(), date.monthOfYear().get(), date.dayOfMonth().get())
  }

  /**
    * Calculates the number of days from the Unix epoch
    * @param y   - year
    * @param m   - month
    * @param day - day
    * @return    - unix epoch day
    */
  def toEpochDay(y: Long, m: Long, day:Long) = {
    val CYCLE_DAYS = 146097
    val DAYS_TO_1970 = (CYCLE_DAYS * 5L) - (30L * 365L + 7L)
    var totalDays: Long = 0
    totalDays += 365 * y
    if (y >= 0) {
      totalDays += (y + 3) / 4 - (y + 99) / 100 + (y + 399) / 400
    }
    else {
      totalDays -= y / -4 - y / -100 + y / -400
    }
    totalDays += ((367 * m - 362) / 12)
    totalDays += day - 1
    if (m > 2) {
      totalDays -= 1

      if (calendar.isLeapYear(y.toInt) == false) {
        totalDays -= 1
      }
    }
    totalDays - DAYS_TO_1970
  }
}