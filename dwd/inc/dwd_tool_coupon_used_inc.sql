-- 建表
drop table if exists dwd_tool_coupon_used_inc;
create external table dwd_tool_coupon_used_inc
(
    `id`           string comment '编号',
    `coupon_id`    string comment '优惠券id',
    `user_id`      string comment '用户id',
    `order_id`     string comment '订单id',
    `date_id`      string comment '日期id',
    `payment_time` string comment '使用(支付)时间'
) comment '优惠券使用（支付）事务事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/gmall/warehouse/dwd/dwd_tool_coupon_used_inc/'
    tblproperties ("orc.compress" = "snappy");


-- 装载数据（首日）
insert overwrite table dwd_tool_coupon_used_inc partition (dt)
select `id`,
       `coupon_id`,
       `user_id`,
       `order_id`,
       `date_id`,
       `payment_time`,
       date_format(data.`used_time`, 'yyyy-MM-dd')
from (select data.`id`,
             data.`coupon_id`,
             data.`user_id`,
             data.`order_id`,
             date_format(data.`used_time`, 'yyyy-MM-dd') `date_id`,
             data.`used_time`                            `payment_time`
      from ods_coupon_use_inc
      where dt = '2024-05-05'
        and type = 'bootstrap-insert'
        and data.used_time is not null) cu;


-- 装载数据（每日）
insert overwrite table dwd_tool_coupon_used_inc partition (dt = '2024-05-06')
select `id`,
       `coupon_id`,
       `user_id`,
       `order_id`,
       `date_id`,
       `payment_time`
from (select data.`id`,
             data.`coupon_id`,
             data.`user_id`,
             data.`order_id`,
             date_format(data.`used_time`, 'yyyy-MM-dd') `date_id`,
             data.`used_time`                            `payment_time`
      from ods_coupon_use_inc
      where dt = '2024-05-06'
        and type = 'update'
        -- and array_contains(map_keys(old), 'used_time')
        and data.used_time is not null) cu;