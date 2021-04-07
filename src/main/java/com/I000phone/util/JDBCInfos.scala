package com.I000phone.util

import java.util.Properties

import com.I000phone.common.CommonConstant

object JDBCInfos {
  def getJdbcInfo(): (String,String,Properties) ={
    val url = ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.URL)
    val tableName = ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.MYSQL_TABLE_NAME)
    val user = ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.USER_NAME)
    val passwd = ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.PASSWORD)
    val driver = ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.DRIVER)
    val properties = new Properties()
    properties.put("user",user)
    properties.put("password",passwd)
    properties.put("driver",driver)

    (url,tableName,properties)

  }
}
