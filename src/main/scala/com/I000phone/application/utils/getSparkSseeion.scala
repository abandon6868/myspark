package com.I000phone.application.utils

import com.I000phone.util.ResourceManagerUtil
import org.apache.spark.sql.SparkSession

object GetSparkSession {
  def getSparkSession(): SparkSession ={
    val builder = SparkSession.builder().appName(GetSparkSession.getClass().getSimpleName)
    // 通过运行模型进行判断，如果为 local模式 设置为 local[*],如果为test production 模式，通过 spark --submit 提交
    if (ResourceManagerUtil.runMode.name().toLowerCase.equals("local")){
      builder.master("local[4]")
    }

    // 启动spark 对hive的支持
    val spark: SparkSession = builder.enableHiveSupport().getOrCreate()
//    // 判断非法值
//
//    if (args != null || args.length ==1){
//      dt = args(0)
//    }

    // 设置spark的日志级别
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    spark
  }
}
