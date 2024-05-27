-- 建表
drop table if exists dwd_traffic_page_view_inc;
create external table dwd_traffic_page_view_inc
(
    `province_id`    string comment '省份id',
    `brand`          string comment '手机品牌',
    `channel`        string comment '渠道',
    `is_new`         string comment '是否首次启动',
    `model`          string comment '手机型号',
    `mid_id`         string comment '设备id',
    `operate_system` string comment '操作系统',
    `user_id`        string comment '会员id',
    `version_code`   string comment 'app版本号',
    `page_item`      string comment '目标id',
    `page_item_type` string comment '目标类型',
    `last_page_id`   string comment '上页id',
    `page_id`        string comment '页面id ',
    `from_pos_id`    string comment '点击坑位id',
    `from_pos_seq`   string comment '点击坑位位置',
    `refer_id`       string comment '营销渠道id',
    `date_id`        string comment '日期id',
    `view_time`      string comment '跳入时间',
    `session_id`     string comment '所属会话id',
    `during_time`    bigint comment '持续时间毫秒'
) comment '流量域页面浏览事务事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/gmall/warehouse/dwd/dwd_traffic_page_view_inc'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
insert overwrite table dwd_traffic_page_view_inc partition (dt = '2024-05-05')
select common.ar                                                           `province_id`,
       common.ba                                                           `brand`,
       common.ch                                                           `channel`,
       common.`is_new`,
       common.md                                                           `model`,
       common.mid                                                          `mid_id`,
       common.os                                                           `operate_system`,
       common.uid                                                          `user_id`,
       common.vc                                                           `version_code`,
       page.item                                                           `page_item`,
       page.item_type                                                      `page_item_type`,
       page.`last_page_id`,
       page.`page_id`,
       page.`from_pos_id`,
       page.`from_pos_seq`,
       page.`refer_id`,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          `date_id`,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') `view_time`,
       common.`session_id`,
       page.`during_time`
from ods_log_inc
where dt = '2024-05-05'
  and page.page_id is not null;