-- 建表
drop table if exists ads_order_by_province;
create external table ads_order_by_province
(
    dt                 string comment '统计日期',
    recent_days        bigint comment '最近天数,1:最近1天,7:最近7天,30:最近30天',
    province_id        string comment '省份id',
    province_name      string comment '省份名称',
    area_code          string comment '地区编码',
    iso_code           string comment '旧版国际标准地区编码，供可视化使用',
    iso_code_3166_2    string comment '新版国际标准地区编码，供可视化使用',
    order_count        bigint comment '订单数',
    order_total_amount decimal(16, 2) comment '订单金额'
) comment '各省份交易统计'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_order_by_province/';


-- 装载数据
insert overwrite table ads_order_by_province
select *
from ads_order_by_province
union
select *
from (select '2024-05-05',
             1,
             province_id,
             province_name,
             area_code,
             iso_code,
             iso_3166_2               iso_code_3166_2,
             order_count_1d           order_count,
             order_original_amount_1d order_total_amount
      from dws_trade_province_order_1d
      where dt = '2024-05-05'
      union all
      select '2024-05-05',
             recent_days,
             province_id,
             province_name,
             area_code,
             iso_code,
             iso_3166_2                                                               iso_code_3166_2,
             if(recent_days = 7, order_count_7d, order_count_30d)                     order_count,
             if(recent_days = 7, order_original_amount_7d, order_original_amount_30d) order_total_amount
      from dws_trade_province_order_nd lateral view explode(array(1, 7, 30)) tmp as recent_days
      where dt = '2024-05-05') t;