-- 建表
drop table if exists dws_traffic_session_page_view_1d;
create external table dws_traffic_session_page_view_1d
(
    session_id     string comment '会话id',
    mid_id         string comment '设备id',
    brand          string comment '手机品牌',
    model          string comment '手机型号',
    operate_system string comment '操作系统',
    version_code   string comment 'app版本号',
    channel        string comment '渠道',
    during_time_1d bigint comment '最近1日浏览时长',
    page_count_1d  bigint comment '最近1日浏览页面数'
) comment '流量域会话粒度页面浏览最近1日汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_traffic_session_page_view_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
insert overwrite table dws_traffic_session_page_view_1d partition (dt = '2024-05-05')
select session_id,
       mid_id,
       brand,
       model,
       operate_system,
       version_code,
       channel,
       sum(during_time),
       count(page_id)
from dwd_traffic_page_view_inc
where dt = '2024-05-05'
group by session_id,
         mid_id,
         brand,
         model,
         operate_system,
         version_code,
         channel;