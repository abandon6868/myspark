package com.I000phone.application.rdbms

import com.I000phone.util.ResourceManagerUtil
import org.apache.spark.sql.SparkSession

object DM2RDBMSApplication {
  var dt = ""

  def commonOperate(args:Array[String]): SparkSession ={
    val builder = SparkSession.builder().appName(DM2RDBMSApplication.getClass.getName())

    if (ResourceManagerUtil.runMode.name().toLowerCase.equals("local")){
      builder.master("local")
    }

    if (args != null && args.length ==1){
      dt = args(0).trim
    }

    val spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    spark
  }
}
