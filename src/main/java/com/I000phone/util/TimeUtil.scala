package com.I000phone.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


object TimeUtil {
  private val sdf = new SimpleDateFormat("yyyyMMdd")
  private val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private val calendar: Calendar = Calendar.getInstance()

  // apply注入方法把String类型的时间转换为Long类型的时间
  def apply(time:String): Long ={
    val date: Date = format.parse(time)
    calendar.setTime(date)
    calendar.getTimeInMillis
  }

  def updateCalendar(amont:Int): Long ={
    calendar.add(Calendar.DATE,amont)
    val time = calendar.getTimeInMillis
//    calendar.add(Calendar.DATE,-amont)
    time
  }

  def updateSpecialCalendar(amout:Int,timeStr:String): Long ={
    calendar.setTime(format.parse(timeStr))
    calendar.add(Calendar.DATE,amout)
    val time = calendar.getTimeInMillis
    time
  }

  val yesterday: String = sdf.format(updateCalendar(-1))

}
