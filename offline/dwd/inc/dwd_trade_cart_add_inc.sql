-- 建表
drop table if exists dwd_trade_cart_add_inc;
create external table dwd_trade_cart_add_inc
(
    id          string comment '编号',
    user_id     string comment '用户id',
    sku_id      string comment 'sku_id',
    date_id     string comment '日期id',
    create_time string comment '加购时间',
    sku_num     bigint comment '加购物车件数'
) comment '交易域加购事务事实表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dwd/dwd_trade_cart_add_inc/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       data.sku_num,
       date_format(data.create_time, 'yyyy-MM-dd')
from ods_cart_info_inc
where dt = '2024-05-05'
  and type = 'bootstrap-insert';


-- 装载数据（每日）
insert overwrite table dwd_trade_cart_add_inc partition (dt = '2024-05-06')
select data.id,
       data.user_id,
       data.sku_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') create_time,
       if(type = 'insert', data.sku_num, data.sku_num - old['sku_num'])
from ods_cart_info_inc
where dt = '2024-05-06'
  and (type = 'insert'
    or (type = 'update'
        and old['sku_num'] is not null
        and cast(data.sku_num as bigint) > cast(old['sku_num'] as bigint)));