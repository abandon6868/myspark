package com.I000phone.util;

import com.I000phone.common.CommonConstant;
import com.I000phone.common.RunMode;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ResourceManagerUtil {
    private static Properties properties;
    public static RunMode runMode;

    static {
        properties = new Properties();
        try {
            InputStream inputStream = ResourceManagerUtil.class.getClassLoader().getResourceAsStream(CommonConstant.COMMON_CONFIG_FILE_NAME);
            properties.load(inputStream);
            runMode = RunMode.valueOf(properties.getProperty(CommonConstant.SPARK_JOB_RUN_MODE).toUpperCase());
            // 合并两个 properties 为一个
            properties.putAll(JDBCUtil.getProperties());
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static String getPropertiesValueByKey(String key){
        return properties.getProperty(key);
    }
}

