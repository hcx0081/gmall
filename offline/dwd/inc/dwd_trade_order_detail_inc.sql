-- 建表
drop table if exists dwd_trade_order_detail_inc;
create external table dwd_trade_order_detail_inc
(
    id                    string comment '编号',
    order_id              string comment '订单id',
    user_id               string comment '用户id',
    sku_id                string comment '商品id',
    province_id           string comment '省份id',
    activity_id           string comment '参与活动id',
    activity_rule_id      string comment '参与活动规则id',
    coupon_id             string comment '使用优惠券id',
    date_id               string comment '下单日期id',
    create_time           string comment '下单时间',
    sku_num               bigint comment '商品数量',
    split_original_amount decimal(16, 2) comment '原始价格',
    split_activity_amount decimal(16, 2) comment '活动优惠分摊',
    split_coupon_amount   decimal(16, 2) comment '优惠券优惠分摊',
    split_total_amount    decimal(16, 2) comment '最终价格分摊'
) comment '交易域下单事务事实表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dwd/dwd_trade_order_detail_inc/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_id,
       create_time,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount,
       date_id
from (select data.id,
             data.order_id,
             data.sku_id,
             data.sku_num,
             data.sku_num * data.order_price    split_original_amount,
             nvl(data.split_activity_amount, 0) split_activity_amount,
             nvl(data.split_coupon_amount, 0)   split_coupon_amount,
             nvl(data.split_total_amount, 0)    split_total_amount
      from ods_order_detail_inc
      where dt = '2024-05-05'
        and type = 'bootstrap-insert') od
         left join (select data.id,
                           data.user_id,
                           data.province_id,
                           date_format(data.create_time, 'yyyy-MM-dd') date_id,
                           data.create_time
                    from ods_order_info_inc
                    where dt = '2024-05-05'
                      and type = 'bootstrap-insert') oi on od.order_id = oi.id
         left join (select data.order_detail_id,
                           data.activity_id,
                           data.activity_rule_id
                    from ods_order_detail_activity_inc
                    where dt = '2024-05-05'
                      and type = 'bootstrap-insert') oda on od.id = oda.order_detail_id
         left join (select data.order_detail_id,
                           data.coupon_id
                    from ods_order_detail_coupon_inc
                    where dt = '2024-05-05'
                      and type = 'bootstrap-insert') odc on od.id = odc.order_detail_id;


-- 装载数据（每日）
insert overwrite table dwd_trade_order_detail_inc partition (dt = '2024-05-06')
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_id,
       create_time,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount
from (select data.id,
             data.order_id,
             data.sku_id,
             data.sku_num,
             data.sku_num * data.order_price    split_original_amount,
             nvl(data.split_activity_amount, 0) split_activity_amount,
             nvl(data.split_coupon_amount, 0)   split_coupon_amount,
             nvl(data.split_total_amount, 0)    split_total_amount
      from ods_order_detail_inc
      where dt = '2024-05-06'
        and type = 'insert') od
         left join (select data.id,
                           data.user_id,
                           data.province_id,
                           date_format(data.create_time, 'yyyy-MM-dd') date_id,
                           data.create_time
                    from ods_order_info_inc
                    where dt = '2024-05-06'
                      and type = 'insert') oi on od.order_id = oi.id
         left join (select data.order_detail_id,
                           data.activity_id,
                           data.activity_rule_id
                    from ods_order_detail_activity_inc
                    where dt = '2024-05-06'
                      and type = 'insert') oda on od.id = oda.order_detail_id
         left join (select data.order_detail_id,
                           data.coupon_id
                    from ods_order_detail_coupon_inc
                    where dt = '2024-05-06'
                      and type = 'insert') odc on od.id = odc.order_detail_id;