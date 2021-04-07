package com.I000phone.application.dws

import com.I000phone.application.dwd.DWDSql
import com.I000phone.util.{ResourceManagerUtil, TimeUtil}
import org.apache.spark.sql.SparkSession

object DWD2DWSApplication {
  var dt:String = ""

  def main(args: Array[String]): Unit = {
    val spark = commonOperate(args)
    loadDataFromHive(spark)

  }

  def loadDataFromHive(spark:SparkSession): Unit ={
    spark.sql("use mtbap_dwd")

    val yesterday = TimeUtil.yesterday
    var targetDate = dt

    if ("".equals(targetDate)){
      targetDate = yesterday
    }

    spark.sql(DWDSql.LOAD_ODS_USER_2_DWD.replace(yesterday, targetDate))
    spark.sql(DWDSql.LOAD_ODS_USER_EXTEND_2_DWD.replace(yesterday, targetDate))
    spark.sql(DWDSql.LOAD_ODS_BIZ_TRADE_2_DWD.replace(yesterday, targetDate))
    spark.sql(DWDSql.LOAD_ODS_CART_2_DWD.replace(yesterday, targetDate))
    spark.sql(DWDSql.LOAD_ODS_CODE_CATEGORY_2_DWD)
    spark.sql(DWDSql.LOAD_ODS_ORDER_DELIVERY_2_DWD.replace(yesterday, targetDate))
    spark.sql(DWDSql.LOAD_ODS_ORDER_ITEM_2_DWD.replace(yesterday, targetDate))
    spark.sql(DWDSql.LOAD_ODS_US_ORDER_2_DWD.replace(yesterday, targetDate))
    spark.sql(DWDSql.LOAD_ODS_USER_APP_PV_2_DWD.replace(yesterday, targetDate))
    spark.sql(DWDSql.LOAD_ODS_USER_PC_PV_2_DWD.replace(yesterday, targetDate))


  }

  def commonOperate(args:Array[String]): SparkSession ={
    val builder: SparkSession.Builder = SparkSession.builder().appName(DWD2DWSApplication.getClass().getName)

    if (ResourceManagerUtil.runMode.name().toLowerCase.equals("local")){
      builder.master("local[*]")
    }

    if (args != null && args.length == 1){
      dt = args(0).trim
    }

    val spark = builder.enableHiveSupport().getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
