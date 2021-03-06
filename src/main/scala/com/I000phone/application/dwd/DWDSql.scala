package com.I000phone.application.dwd

import com.I000phone.util.TimeUtil

object DWDSql {
    // ------------↓    加载ods层表的数据到dwd层相应表中  ↓------------
    //将分区字段dt的值默认设置为上一天
    var dt:String=TimeUtil.yesterday
//    var dt:String=""
    /**
     * load data to dwd_user
     */
    val LOAD_ODS_USER_2_DWD: String =
      s"""
         |insert overwrite table mtbap_dwd.dwd_user partition(dt=$dt)
         |select
         |	t.user_id,
         |	t.user_name,
         |	t.user_gender,
         |	t.user_birthday,
         |	t.user_age,
         |	t.constellation,
         |	t.province,
         |	t.city,
         |	t.city_level,
         |	t.e_mail,
         |	t.op_mail,
         |	t.mobile,
         |	t.num_seg_mobile,
         |	t.op_mobile,
         |	t.register_time,
         |	t.login_ip,
         |	t.login_source,
         |	t.request_user,
         |	t.total_score,
         |	t.used_score,
         |	t.is_blacklist,
         |	t.is_married,
         |	t.education,
         |	t.monthly_income,
         |	t.profession,
         |	current_timestamp() dw_date
         |from mtbap_ods.ods_user t
         |where dt=$dt
    """.stripMargin


    // load data to user_extend
    val LOAD_ODS_USER_EXTEND_2_DWD: String =
      s"""
         |insert overwrite table mtbap_dwd.dwd_user_extend  partition(dt=$dt)
         |SELECT
         |	user_id,
         |	user_gender,
         |	is_pregnant_woman,
         |	is_have_children,
         |	is_have_car,
         |	phone_brand,
         |	phone_brand_level,
         |	phone_cnt,
         |	change_phone_cnt,
         |	is_maja,
         |	majia_account_cnt,
         |	loyal_model,
         |	shopping_type_model,
         |	weight,
         |	height,
         |	current_timestamp()
         |from mtbap_ods.ods_user_extend t
         |where dt=$dt
    """.stripMargin

    /**
     * load data to biz_tade
     */
    val LOAD_ODS_BIZ_TRADE_2_DWD: String =
      s"""
         |insert overwrite table mtbap_dwd.dwd_biz_trade  partition(dt=$dt)
         |select
         |	trade_id,
         |	order_id,
         |	user_id,
         |	amount,
         |	trade_type,
         |	trade_time,
         |	current_timestamp() dw_date
         |from mtbap_ods.ods_biz_trade
         |where dt=$dt
    """.stripMargin

    /**
     * load data to cart
     */
    val LOAD_ODS_CART_2_DWD: String =
      s"""
         |  insert overwrite table dwd_cart partition(dt=$dt)
         |  select
         |  cart_id,
         |  session_id,
         |  user_id,
         |  goods_id,
         |  goods_num,
         |  add_time,
         |  cancle_time,
         |  sumbit_time,
         |  create_date,
         |  current_timestamp() dw_dt
         |    from mtbap_ods.ods_cart
         |  where dt=$dt
    """.stripMargin

    /**
     * load data to code_category
     */
    val LOAD_ODS_CODE_CATEGORY_2_DWD: String =
      """
        |   insert overwrite table dwd_code_category
        |   select
        |   first_category_id,
        |   first_category_name,
        |   second_category_id,
        |   second_catery_name,
        |   third_category_id,
        |   third_category_name,
        |   category_id,
        |   current_timestamp() dw_dt
        |     from mtbap_ods.ods_code_category
    """.stripMargin

    /**
     * load data to order-delivery,将筛选之后指定的日期的数据从ods层hive表中导入到dwd层相应的分区表中，分区标识字段即为：指定的日期
     */
    val LOAD_ODS_ORDER_DELIVERY_2_DWD: String =
      s"""
         |   insert overwrite table dwd_order_delivery partition(dt=$dt)
         |   select
         |   order_id,
         |   order_no,
         |   consignee,
         |   area_id,
         |   area_name,
         |   address,
         |   mobile,
         |   phone,
         |   coupon_id,
         |   coupon_money,
         |   carriage_money,
         |   create_time,
         |   update_time,
         |   addr_id,
         |   current_timestamp() dw_dt
         |   from mtbap_ods.ods_order_delivery
         |   where dt=$dt
    """.stripMargin

    /**
     * load data to order_item
     */
    val LOAD_ODS_ORDER_ITEM_2_DWD: String =
      s"""
         |   insert overwrite table dwd_order_item partition(dt=$dt)
         |   select
         |   user_id,
         |   order_id,
         |   order_no,
         |   goods_id,
         |   goods_no,
         |   goods_name,
         |   goods_amount,
         |   shop_id,
         |   shop_name,
         |   curr_price,
         |   market_price,
         |   discount,
         |   cost_price,
         |   first_cart,
         |   first_cart_name,
         |   second_cart,
         |   second_cart_name,
         |   third_cart,
         |   third_cart_name,
         |   goods_desc,
         |   current_timestamp() dw_dt
         |     from mtbap_ods.ods_order_item
         |   where dt=$dt
    """.stripMargin

    /**
     * load data to order
     */
    val LOAD_ODS_US_ORDER_2_DWD: String =
      s"""
         |  insert overwrite table dwd_us_order partition(dt=$dt)
         |  select
         |  order_id,
         |  order_no,
         |  order_date,
         |  user_id,
         |  user_name,
         |  order_money,
         |  order_type,
         |  order_status,
         |  pay_status,
         |  pay_type,
         |  order_source,
         |  update_time,
         |  current_timestamp() dw_dt
         |    from mtbap_ods.ods_us_order
         |  where dt=$dt
    """.stripMargin


    /**
     * load data to user_app_pv
     */
    val LOAD_ODS_USER_APP_PV_2_DWD: String =
      s"""
         |  insert overwrite table dwd_user_app_pv partition(dt=$dt)
         |  select
         |  log_id,
         |  user_id,
         |  imei,
         |  log_time,
         |  hour(log_time) log_hour,
         |  visit_os,
         |  os_version,
         |  app_name,
         |  app_version,
         |  device_token,
         |  visit_ip,
         |  province,
         |  city,
         |  current_timestamp() dw_date
         |    from mtbap_ods.ods_user_app_click_log
         |  where dt=$dt
    """.stripMargin

    /**
     * load data to user_pc_pv
     *
     * dwd 层dwd_user_pc_pv表中保存的是一个用户在一个session范围内通过pc端访问‘饿了么’外卖平台聚合后的结果
     * 如：开始访问时间， 结束访问时间， 访问总时长，页面总数（pv数）
     */
    val LOAD_ODS_USER_PC_PV_2_DWD: String =
      s"""
         |   insert overwrite table dwd_user_pc_pv partition(dt=$dt)
         |   SELECT
         |   max(log_id),
         |   user_id,
         |   session_id,
         |   cookie_id,
         |   min(visit_time) in_time,
         |   max(visit_time) out_time,
         |   -- 本次用户访问的时长
         |   case when min(visit_time) = max(visit_time) then 3  else unix_timestamp(max(visit_time)) -  unix_timestamp(min(visit_time)) end stay_time,
         |   count(1) pv,
         |   -- 存储过程造的数据目测有问题。
         |   visit_os,
         |   browser_name,
         |   visit_ip,
         |   province,
         |   city,
         |   current_timestamp() dw_date
         |     from mtbap_ods.ods_user_pc_click_log
         |   where dt=$dt
         |   group by
         |     user_id,
         |   cookie_id,
         |   session_id,
         |   visit_os,
         |   browser_name,
         |   visit_ip,
         |   province,
         |   city
    """.stripMargin
}
