package com.I000phone.application.ods

object ODSSql {
  // ------------↓    加载dwd层表的数据到dws层相应表中  ↓------------

  /**
   * load data osd层的表ods_order_delivery
   */
  val LOAD_RDBMS_2_ODS_ORDER_DELIVERY: String =
    """
      |insert overwrite table qfbap_ods.ods_order_delivery partition(dt=?)
      |
      |select
      | *
      | from
      | tmp_order_delivery
    """.stripMargin
}
