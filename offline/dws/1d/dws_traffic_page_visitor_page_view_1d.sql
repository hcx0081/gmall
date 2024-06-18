-- 建表
drop table if exists dws_traffic_page_visitor_page_view_1d;
create external table dws_traffic_page_visitor_page_view_1d
(
    mid_id         string comment '访客id',
    brand          string comment '手机品牌',
    model          string comment '手机型号',
    operate_system string comment '操作系统',
    page_id        string comment '页面id',
    during_time_1d bigint comment '最近1日浏览时长',
    view_count_1d  bigint comment '最近1日访问次数'
) comment '流量域访客页面粒度页面浏览最近1日汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_traffic_page_visitor_page_view_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
insert overwrite table dws_traffic_page_visitor_page_view_1d partition (dt = '2024-05-05')
select mid_id,
       brand,
       model,
       operate_system,
       page_id,
       sum(during_time),
       count(*)
from dwd_traffic_page_view_inc
where dt = '2024-05-05'
group by mid_id,
         brand,
         model,
         operate_system,
         page_id