-- 建表
drop table if exists dws_trade_user_cart_add_1d;
create external table dws_trade_user_cart_add_1d
(
    user_id           string comment '用户id',
    cart_add_count_1d bigint comment '最近1日加购次数',
    cart_add_num_1d   bigint comment '最近1日加购商品件数'
) comment '交易域用户粒度加购最近1日汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_trade_user_cart_add_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dws_trade_user_cart_add_1d partition (dt)
select user_id,
       count(*),
       sum(sku_num),
       dt
from dwd_trade_cart_add_inc
group by dt, user_id;


-- 装载数据（每日）
insert overwrite table dws_trade_user_cart_add_1d partition (dt = '2024-05-06')
select user_id,
       count(*),
       sum(sku_num)
from dwd_trade_cart_add_inc
where dt = '2024-05-06'
group by user_id;