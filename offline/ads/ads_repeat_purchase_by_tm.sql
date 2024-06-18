-- 建表
drop table if exists ads_repeat_purchase_by_tm;
create external table ads_repeat_purchase_by_tm
(
    dt                string comment '统计日期',
    recent_days       bigint comment '最近天数,30:最近30天',
    tm_id             string comment '品牌id',
    tm_name           string comment '品牌名称',
    order_repeat_rate decimal(16, 2) comment '复购率'
) comment '最近30日各品牌复购率统计'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_repeat_purchase_by_tm/';


-- 装载数据
insert overwrite table ads_repeat_purchase_by_tm
select *
from ads_repeat_purchase_by_tm
union
select '2024-05-05',
       30,
       tm_id,
       tm_name,
       sum(if(order_count > 1, 1, 0)) / count(user_id) * 100
from (select user_id,
             tm_id,
             tm_name,
             sum(order_count_30d) order_count
      from dws_trade_user_sku_order_nd
      where dt = '2024-05-05'
      group by user_id, tm_id, tm_name) t
group by tm_id, tm_name;