package com.I000phone.application.dws

import com.I000phone.util.TimeUtil

object DWSSql {
  // ------------↓    加载dwd层表的数据到dws层相应表中  ↓------------
  //将分区字段dt的值默认设置为上一天
  var dt: String = TimeUtil.yesterday

  /**
   * load data dws_user_visit_month1
   */
  val LOAD_DWD_2_DWS_USER_VISIT_MONTH1: String =
    s"""
       | insert overwrite table mtbap_dws.dws_user_visit_month1 partition(dt=$dt)
       |
       |select
       |	t.user_id,
       |	t.type,
       |	t.content,
       |	t.cnt,
       |	row_number() over(distribute by t.user_id,t.type sort by t.cnt desc) rn,
       |	current_timestamp() dw_date
       |from (
       |		select
       |		  user_id,
       |		  'visit_ip' as type,-- 近30天访问ip
       |		  sum(pv) as cnt,
       |		  visit_ip as content
       |		from mtbap_dwd.dwd_user_pc_pv
       |		where dt >= date_format(date_sub(from_unixtime(unix_timestamp('$dt','yyyyMMdd'),'yyyy-MM-dd'),29),'yyyyMMdd')
       |		group by
       |			user_id,
       |			visit_ip
       |
       |		union all
       |
       |		select
       |		  user_id,
       |		  'cookie_id' as type, -- 近30天常用cookie
       |		  sum(pv) as cnt,
       |		  cookie_id as content
       |		from mtbap_dwd.dwd_user_pc_pv
       |		where dt >= date_format(date_sub(from_unixtime(unix_timestamp('$dt','yyyyMMdd'),'yyyy-mm-dd'),29),'yyyyMMdd')
       |		group by
       |			user_id,
       |			cookie_id
       |
       |		union all
       |
       |		select
       |		  user_id,
       |		  'browser_name' as type,-- 近30天常用浏览器
       |		  sum(pv) as cnt,
       |		  browser_name as content
       |		from mtbap_dwd.dwd_user_pc_pv
       |		where dt >= date_format(date_sub(from_unixtime(unix_timestamp('$dt','yyyyMMdd'),'yyyy-mm-dd'),29),'yyyyMMdd')
       |		group by
       |			user_id,
       |			browser_name
       |
       |		union all
       |
       |		select
       |		  user_id,
       |		  'visit_os' as type, -- 近30天常用操作系统
       |		  sum(pv) as cnt,
       |		  visit_os as content
       |		from mtbap_dwd.dwd_user_pc_pv
       |		where dt >= date_format(date_sub(from_unixtime(unix_timestamp('$dt','yyyyMMdd'),'yyyy-mm-dd'),29),'yyyyMMdd')
       |		group by
       |			user_id,
       |			visit_os
       |	) t
    """.stripMargin

  /**
   * load data load data dw_user_basic
   */
  val LOAD_DWD_2_DWS_USER_BASIC: String =
    s"""
       |insert overwrite table mtbap_dws.dws_user_basic  partition(dt=$dt)
       |select
       |   a.user_id          ,
       |   a.user_name        ,
       |   a.user_gender      ,
       |   a.user_birthday    ,
       |   a.user_age         ,
       |   a.constellation    ,
       |   a.province         ,
       |   a.city             ,
       |   a.city_level       ,
       |   a.e_mail             ,
       |   a.op_mail          ,
       |   a.mobile           ,
       |   a.num_seg_mobile     ,
       |   a.op_mobile        ,
       |   a.register_time    ,
       |   a.login_ip         ,
       |   a.login_source     ,
       |   a.request_user     ,
       |   a.total_score      ,
       |   a.used_score       ,
       |   a.is_blacklist     ,
       |   a.is_married       ,
       |   a.education        ,
       |   a.monthly_income  ,
       |   a.profession       ,
       |
       |   b.is_pregnant_woman,
       |   b.is_have_children ,
       |   b.is_have_car      ,
       |   b.phone_brand      ,
       |   b.phone_brand_level,
       |   b.phone_cnt        ,
       |   b.change_phone_cnt ,
       |   b. is_maja          ,
       |   b.majia_account_cnt,
       |   b.loyal_model      ,
       |   b.shopping_type_model,
       |   b.weight           ,
       |   b.height           ,
       |
       |   b.dw_date
       |from mtbap_dwd.dwd_user a
       |left join mtbap_dwd.dwd_user_extend b on a.user_id = b.user_id
       |where a.dt=$dt and  b.dt=$dt
    """.stripMargin

}
