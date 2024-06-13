-- 建表
drop table if exists ads_order_to_pay_interval_avg;
create external table ads_order_to_pay_interval_avg
(
    dt                        string comment '统计日期',
    order_to_pay_interval_avg bigint comment '下单到支付时间间隔平均值,单位为秒'
) comment '下单到支付时间间隔平均值统计'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_order_to_pay_interval_avg/';


-- 装载数据
select '2024-05-05',
       avg(unix_timestamp(payment_time) - unix_timestamp(order_time))
from dwd_trade_trade_flow_acc
where (dt = '2024-05-05' or dt = '9999-12-31')
  and payment_date_id = '2024-05-05';