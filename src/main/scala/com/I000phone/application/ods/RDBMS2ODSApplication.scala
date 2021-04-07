package com.I000phone.application.ods

import java.util.Properties

import com.I000phone.application.utils.GetSparkSession
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.I000phone.common.CommonConstant
import com.I000phone.util.{JDBCInfos, ResourceManagerUtil}

/**
 * 使用sparkSql实现 数据从 rdbms-> ods
 */
object RDBMS2ODSApplication {
  var dt = ""
  def main(args: Array[String]): Unit = {
    // 1 创建spark实例对象
    val spark = commonOperate(args)

    // 2 rdbms -> mysql 全量导入
    fullImportDataToODS(spark,ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME_CODE_CATEGORY),"mtbap_ods.ods_code_category")
    //    fullImportDataToODS(spark, ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME_CODE_CITY), "mtbap_ods.ods_code_city")
    //    fullImportDataToODS(spark, ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME_CODE_EDUCATION), "mtbap_ods.ods_code_education")
    //    fullImportDataToODS(spark, ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME_CODE_EMAIL_SUFFIX), "mtbap_ods.code_email_suffix")
    //    fullImportDataToODS(spark, ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME_CODE_GOODS), "mtbap_ods.code_goods")
    //    fullImportDataToODS(spark, ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME_CODE_PROFESSION), "mtbap_ods.code_profession")
    //    fullImportDataToODS(spark, ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME_CODE_SHOP), "mtbap_ods.code_shop")


    //增量导入（读取hive表中所有的数据，与mysql中的表依次进行比对，将hive表中不存在的数据实现增量导入操作）
    /***
     *  要求:
     *      思路一： mysql 中的字段需要有更新时间或创建时间，使用时间dt来判断数据是否更新
     *      思路二： 使用canal工具，监控mysal表的更新，并将数据写入一个临时库中，spark去读取临时库中的数据
     *      思路三： 分别读取mysql与hive表中的数据做对比，保存不一样的数据 -- 这种风险系数较大，想法比较蠢
     */
        fullImportDataToODS(spark,ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME_USER),"mtbap_ods.ods_user")
    //    fullImportDataToODS(spark,ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME_USER_EXTEND),"mtbap_ods.ods_user_extend")
    // fullImportDataToODS(spark, ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME_USER_ADDR), "mtbap_ods.ods_user_addr")


    //ods层中有分区的表实现RDBMS ~ >ODS
    //a) 读取RDBMS 中只读db中的业务表中的数据，装载到内存，映射为虚拟表
    //    var connectionProperties: Properties = new Properties
    //    connectionProperties.put("user", ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.ONLY_READ_DB_USERNAME))
    //    connectionProperties.put("password", ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.ONLY_READ_DB_PASSWORD))
    //    connectionProperties.put("driver", ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.ONLY_READ_DB_DRIVERCLASSNAME))
    //    var deliveryDF: DataFrame = spark.read.format("jdbc").jdbc(ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.ONLY_READ_DB_URL),
    //      "order_delivery", connectionProperties)
    //
    //    deliveryDF.createOrReplaceTempView("tmp_order_delivery")

    //b)准备sql
    //将
    //c) 执行
     spark.sql(ODSSql.LOAD_RDBMS_2_ODS_ORDER_DELIVERY.replace("?", args(0).trim))
    // 关闭spark连接，释放资源
    spark.close()
  }


  /***
   * 全量导入
   * @param spark
   * @param mysqlTable
   * @param hiveTable
   */
  def fullImportDataToODS(spark:SparkSession,mysqlTable:String,hiveTable:String): Unit ={
    // 1 读取RDBMS 中只读db中的业务表中的数据，装载到内存，映射为虚拟表
    val tuple = JDBCInfos.getJdbcInfo()
    // 2 读取mysql指定表中的数据
    val df: DataFrame = spark.read.format("jdbc").jdbc(tuple._1,tuple._2,tuple._3)
    // 3 对hive数仓的操作，将上一步中虚拟表中的数据其写入到hive ods层相应的表中
    df.write.mode(SaveMode.Overwrite).insertInto(hiveTable)
  }

  /**
   *  spark sql的通用配置
   * @param args 传入的参数
   * @return spark 对象
   */
  def commonOperate(args:Array[String]): SparkSession ={
    val builder = SparkSession.builder().appName(RDBMS2ODSApplication.getClass().getName)
    // 通过运行模型进行判断，如果为 local模式 设置为 local[*],如果为test production 模式，通过 spark --submit 提交
    if (ResourceManagerUtil.runMode.name().toLowerCase.equals("local")){
      builder.master("local[4]")
    }

    // 启动spark 对hive的支持
    val spark: SparkSession = builder.enableHiveSupport().getOrCreate()
    // 判断非法值
    if (args != null || args.length ==1){
      dt = args(0)
    }

    // 设置spark的日志级别
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    spark
  }
}
