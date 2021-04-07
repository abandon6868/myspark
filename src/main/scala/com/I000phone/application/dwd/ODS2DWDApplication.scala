package com.I000phone.application.dwd

import com.I000phone.util.{ResourceManagerUtil, TimeUtil}
import org.apache.spark.sql.SparkSession

object ODS2DWDApplication {
  var dt: String = ""

  def main(args: Array[String]): Unit = {

    val spark = commonOperate(args: Array[String])
    loadDataFromHive(spark,dt)

  }

  def loadDataFromHive(spark: SparkSession, dt: String): Unit = {
    spark.sql("use mtbap_dwd")

    // 使用hql语句从ods层将表数据筛选到dwd层
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

  def commonOperate(args: Array[String]): SparkSession = {
    var builder = SparkSession.builder().appName(ODS2DWDApplication.getClass.getName)

    if (ResourceManagerUtil.runMode.name().toLowerCase.equals("local")) {
      builder = builder.master("local[*]")
    }

    if (args != null && args.length == 1) {
      dt = args(0)
    }

//    val spark = builder.getOrCreate()
    val spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    spark

  }


}
