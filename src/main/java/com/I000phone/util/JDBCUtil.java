package com.I000phone.util;

import com.I000phone.common.CommonConstant;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class JDBCUtil {
    private static Properties properties;

    /**
     *  连接池实例
     */

    private static DataSource ds;

    static {
        properties = new Properties();
        // 根据 job的运行模式。获得对应的目录
        String runMode = ResourceManagerUtil.runMode.name().toLowerCase();
        try {
            String path = runMode + File.separator + CommonConstant.DBCP_CONN_FILE_NAME;
            InputStream inputStream = JDBCUtil.class.getClassLoader().getResourceAsStream(path);
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static Properties getProperties() {return properties;}
}
