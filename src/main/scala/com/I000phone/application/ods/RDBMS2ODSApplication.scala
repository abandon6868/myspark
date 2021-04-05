package com.I000phone.application.ods

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.I000phone.common.CommonConstant
import com.I000phone.util.ResourceManagerUtil

/**
 * 使用sparkSql实现 数据从 rdbms-> ods
 */
object RDBMS2ODSApplication {
  def main(args: Array[String]): Unit = {
    // 1 创建spark实例对象
    val spark = commonOperate(args)

    // 2 rdbms -> mysql
    fullImportDataToODS(spark,ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME_CODE_CATEGORY),"mtbap_ods.ods_code_category")

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
    val connProperties: Properties = new Properties()
    val url: String = ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.URL)
    val userName: String = ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.USER_NAME)
    val password: String = ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.ONLY_READ_DB_PASSWORD)
    val driver: String = ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.DRIVER)

    connProperties.put("url",url)
    connProperties.put("user",userName)
    connProperties.put("password",password)
    connProperties.put("driver",driver)

    // 2 读取mysql指定表中的数据
    val df: DataFrame = spark.read.format("jdbc").jdbc(url, mysqlTable, connProperties)
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
//    if (args == null || args.length !=1){
//      println("请传入分区表示的字段：dt")
//      System.exit(-1)
//    }

    // 设置spark的日志级别
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    spark
  }
}
