package com.I000phone.application.rdbms

import com.I000phone.util.{JDBCInfos, ResourceManagerUtil, TimeUtil}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DM2RDBMSApplication {
  var dt = ""

  def main(args: Array[String]): Unit = {
    val spark = commonOperate(args)
    dM2RDBMSApplication(spark)
  }

  def dM2RDBMSApplication(spark: SparkSession): Unit = {

    val yesterday = TimeUtil.yesterday
    var target = dt
    if ("".equals(target)) {
      target = yesterday
    }
    var dm_2_rdbms_sql: String = s"select * from mtbap_dm.dm_user_visit where dt=$target"

    val df: DataFrame = spark.sql(dm_2_rdbms_sql)
    val tuple = JDBCInfos.getJdbcInfo()

    df.write.mode(SaveMode.Overwrite).jdbc(tuple._1, tuple._2, tuple._3)

    spark.close()
  }

  def commonOperate(args: Array[String]): SparkSession = {
    val builder = SparkSession.builder().appName(DM2RDBMSApplication.getClass.getName())

    if (ResourceManagerUtil.runMode.name().toLowerCase.equals("local")) {
      builder.master("local")
    }

    if (args != null && args.length == 1) {
      dt = args(0).trim
    }

    val spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    spark
  }
}
