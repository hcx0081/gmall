-- 建表
drop table if exists dws_trade_user_order_1d;
create external table dws_trade_user_order_1d
(
    user_id                   string comment '用户id',
    order_count_1d            bigint comment '最近1日下单次数',
    order_num_1d              bigint comment '最近1日下单商品件数',
    order_original_amount_1d  decimal(16, 2) comment '最近1日下单原始金额',
    activity_reduce_amount_1d decimal(16, 2) comment '最近1日下单活动优惠金额',
    coupon_reduce_amount_1d   decimal(16, 2) comment '最近1日下单优惠券优惠金额',
    order_total_amount_1d     decimal(16, 2) comment '最近1日下单最终金额'
) comment '交易域用户粒度订单最近1日汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_trade_user_order_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dws_trade_user_order_1d partition (dt)
select user_id,
       count(distinct order_id)   order_count_1d,
       count(sku_id)              order_num_1d,
       sum(split_original_amount) order_original_amount_1d,
       sum(split_activity_amount) activity_reduce_amount_1d,
       sum(split_coupon_amount)   coupon_reduce_amount_1d,
       sum(split_total_amount)    order_total_amount_1d,
       dt
from dwd_trade_order_detail_inc
group by dt, user_id;


-- 装载数据（每日）
insert overwrite table dws_trade_user_order_1d partition (dt = '2024-05-06')
select user_id,
       count(distinct order_id)   order_count_1d,
       count(sku_id)              order_num_1d,
       sum(split_original_amount) order_original_amount_1d,
       sum(split_activity_amount) activity_reduce_amount_1d,
       sum(split_coupon_amount)   coupon_reduce_amount_1d,
       sum(split_total_amount)    order_total_amount_1d
from dwd_trade_order_detail_inc
where dt = '2024-05-06'
group by user_id;