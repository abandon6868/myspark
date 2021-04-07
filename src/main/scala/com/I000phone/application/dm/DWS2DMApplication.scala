package com.I000phone.application.dm

import com.I000phone.util.{ResourceManagerUtil, TimeUtil, TimeUtils}
import org.apache.spark.sql.SparkSession

object DWS2DMApplication {
  var dt = ""
  def main(args: Array[String]): Unit = {
    val spark = commonOperate(args)
    performOperationOnHive(spark)
  }

  def performOperationOnHive(spark:SparkSession): Unit ={
    spark.sql("USE mtbap_dm")

    val yesterday = TimeUtil.yesterday
    var targetDate:String = dt

    if ("".equals(targetDate)){
      targetDate = yesterday
    }

    val dm_sql=DMSql.LOAD_DWS_2_DM_USER_VISIT.replace(yesterday,targetDate)
    spark.sql(dm_sql)
  }

  def commonOperate(args:Array[String]): SparkSession ={
    val builder = SparkSession.builder().appName(DWS2DMApplication.getClass.getName())

    if (ResourceManagerUtil.runMode.name().toLowerCase.equals("local")){
      builder.master("local[*]")
    }

    //
    if (args != null && args.length ==1){
      dt = args(0)
    }

    val spark = builder.enableHiveSupport().getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")
    spark
  }

}
