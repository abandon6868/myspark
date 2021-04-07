package com.I000phone.util

import java.util.Calendar

import org.apache.commons.lang3.time.FastDateFormat

/***
 * 时间工具类
 */
object TimeUtils {

  //下述的实例是用来代替JDK自带的SimpleDateFormat, 在多线程并发访问的前提下，会有冲突
  private val fastDateFormat: FastDateFormat =
  FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val dtFormat: FastDateFormat =
    FastDateFormat.getInstance("yyyyMMdd")

  /**
   * 日历类
   */
  private val calendar: Calendar = Calendar.getInstance

  // apply注入方法把String类型的时间转换为Long类型的时间
  def apply(time: String): Long = {
    calendar.setTime(fastDateFormat.parse(time))
    calendar.getTimeInMillis
  }

  // 通过Calendar改变日期，以当前时间为准
  def updateCalendar(amout: Int): Long = {
    calendar.add(Calendar.DATE, amout)
    val time = calendar.getTimeInMillis
    time
  }

  // 通过Calendar改变日期，以给定的时间为准
  def updateSpecialCalendar(amout: Int,timeStr:String): Long = {
    calendar.setTime(fastDateFormat.parse(timeStr))
    calendar.add(Calendar.DATE, amout)
    val time = calendar.getTimeInMillis
    time
  }
  val yesterday = TimeUtils.dtFormat.format(TimeUtils.updateCalendar(-1))

//  def main(args: Array[String]): Unit = {
//    val l = updateCalendar(1)
//    println(dtFormat.format(l))
//    val l1 = updateSpecialCalendar(1, "2021-04-03 10:22:33")
//    println(dtFormat.format(l1))
//
//  }

}
