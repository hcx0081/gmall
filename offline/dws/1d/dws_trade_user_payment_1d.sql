-- 建表
drop table if exists dws_trade_user_payment_1d;
create external table dws_trade_user_payment_1d
(
    user_id           string comment '用户id',
    payment_count_1d  bigint comment '最近1日支付次数',
    payment_num_1d    bigint comment '最近1日支付商品件数',
    payment_amount_1d decimal(16, 2) comment '最近1日支付金额'
) comment '交易域用户粒度支付最近1日汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_trade_user_payment_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dws_trade_user_payment_1d partition (dt)
select user_id,
       count(distinct order_id),
       sum(sku_num),
       sum(split_payment_amount),
       dt
from dwd_trade_pay_detail_suc_inc
group by dt, user_id;


-- 装载数据（每日）
insert overwrite table dws_trade_user_payment_1d partition (dt = '2024-05-06')
select user_id,
       count(distinct order_id),
       sum(sku_num),
       sum(split_payment_amount)
from dwd_trade_pay_detail_suc_inc
where dt = '2024-05-06'
group by user_id;