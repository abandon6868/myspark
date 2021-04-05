package com.I000phone.common;

/***
 *  通用接口
 */
public interface CommonConstant {
    // 共同配置文件名
    String COMMON_CONFIG_FILE_NAME = "config.properties";
    // JDBC 连接池
    String DBCP_CONN_FILE_NAME = "jdbc.config.properties";
    // 结果DB 连接相关参数
    String URL = "url";
    String USER_NAME = "username";
    /**
     * password
     */
    String PASSWORD = "password";
    /**
     * driver
     */
    String DRIVER = "driverClassName";

    // 只读 DB中相关的连接参数
    String ONLY_READ_DB_URL = "only.read.db.url";
    String ONLY_READ_DB_USERNAME = "only.read.db.username";
    String ONLY_READ_DB_PASSWORD = "only.read.db.password";
    /**
     * driver
     */
    String ONLY_READ_DB_DRIVERCLASSNAME = "only.read.db.driverClassName";

    // 目标表明
    String MYSQL_TABLE_NAME = "mysql.table.name";

    /**
     * 表名(全量导入)
     */
    String MYSQL_TABLE_NAME_CODE_CITY = "mysql.table.name.code.city";
    String MYSQL_TABLE_NAME_CODE_EDUCATION = "mysql.table.name.code.education";
    String MYSQL_TABLE_NAME_CODE_EMAIL_SUFFIX= "mysql.table.name.code.email.suffix";
    String MYSQL_TABLE_NAME_CODE_GOODS= "mysql.table.name.code.goods";
    String MYSQL_TABLE_NAME_CODE_PROFESSION= "mysql.table.name.code.profession";
    String MYSQL_TABLE_NAME_CODE_SHOP= "mysql.table.name.code.shop";


    /**
     * 表名(增量导入)
     */
    String MYSQL_TABLE_NAME_CODE_CATEGORY = "mysql.table.name.code.category";
    String MYSQL_TABLE_NAME_USER = "mysql.table.name.user";
    String MYSQL_TABLE_NAME_USER_EXTEND = "mysql.table.name.user.extend";
    String MYSQL_TABLE_NAME_USER_ADDR = "mysql.table.name.user.addr";
    String MYSQL_TABLE_NAME_US_ORDER = "mysql.table.name.us.order";
    String MYSQL_TABLE_NAME_CART = "mysql.table.name.cart";
    String MYSQL_TABLE_NAME_ORDER_DELIVERY = "mysql.table.name.order.delivery";
    String MYSQL_TABLE_NAME_ORDER_ITEM = "mysql.table.name.order.item";
    String MYSQL_TABLE_NAME_USER_APP_CLICK_LOG = "mysql.table.name.user.app.click.log";
    String MYSQL_TABLE_NAME_USER_PC_CLICK_LOG = "mysql.table.name.user.pc.click.log";
    String MYSQL_TABLE_NAME_BIZ_TRADE= "mysql.table.name.biz.trade";

    // spark 项目运行模式
    String SPARK_JOB_RUN_MODE = "spark.job.run.mode";

}
