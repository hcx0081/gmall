drop table if exists ads_traffic_stats_by_channel;
create external table ads_traffic_stats_by_channel
(
    `dt`               string comment '统计日期',
    `recent_days`      bigint comment '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `channel`          string comment '渠道',
    `uv_count`         bigint comment '访客人数',
    `avg_duration_sec` bigint comment '会话平均停留时长，单位为秒',
    `avg_page_count`   bigint comment '会话平均浏览页面数',
    `sv_count`         bigint comment '会话数',
    `bounce_rate`      decimal(16, 2) comment '跳出率'
) comment '各渠道流量统计'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_traffic_stats_by_channel/';