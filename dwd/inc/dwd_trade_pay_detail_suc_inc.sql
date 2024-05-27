-- 建表
drop table if exists dwd_trade_pay_detail_suc_inc;
create external table dwd_trade_pay_detail_suc_inc
(
    `id`                    string comment '编号',
    `order_id`              string comment '订单id',
    `user_id`               string comment '用户id',
    `sku_id`                string comment 'sku_id',
    `province_id`           string comment '省份id',
    `activity_id`           string comment '参与活动id',
    `activity_rule_id`      string comment '参与活动规则id',
    `coupon_id`             string comment '使用优惠券id',
    `payment_type_code`     string comment '支付类型编码',
    `payment_type_name`     string comment '支付类型名称',
    `date_id`               string comment '支付日期id',
    `callback_time`         string comment '支付成功时间',
    `sku_num`               bigint comment '商品数量',
    `split_original_amount` decimal(16, 2) comment '应支付原始金额',
    `split_activity_amount` decimal(16, 2) comment '支付活动优惠分摊',
    `split_coupon_amount`   decimal(16, 2) comment '支付优惠券优惠分摊',
    `split_payment_amount`  decimal(16, 2) comment '支付金额'
) comment '交易域支付成功事务事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/gmall/warehouse/dwd/dwd_trade_pay_detail_suc_inc/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select od.`id`,
       od.`order_id`,
       `user_id`,
       `sku_id`,
       `province_id`,
       `activity_id`,
       `activity_rule_id`,
       `coupon_id`,
       `payment_type_code`,
       `payment_type_name`,
       `date_id`,
       `callback_time`,
       `sku_num`,
       `split_original_amount`,
       `split_activity_amount`,
       `split_coupon_amount`,
       `split_payment_amount`,
       `date_id`
from (select data.`id`,
             data.`order_id`,
             data.`sku_id`,
             data.`sku_num`,
             data.`sku_num` * data.`order_price` `split_original_amount`,
             data.`split_activity_amount`,
             data.`split_coupon_amount`
      from ods_order_detail_inc
      where dt = '2024-05-05'
        and type = 'bootstrap-insert') od
         join (select data.`order_id`,
                      data.`payment_type`                             `payment_type_code`,
                      data.`total_amount`                             `split_payment_amount`,
                      date_format(data.`callback_time`, 'yyyy-MM-dd') `date_id`,
                      data.`callback_time`
               from ods_payment_info_inc
               where dt = '2024-05-05'
                 and type = 'bootstrap-insert'
                 and data.payment_status = '1602') pi on od.id = pi.order_id
         left join (select dic_code,
                           dic_name `payment_type_name`
                    from ods_base_dic_full
                    where dt = '2024-05-05'
                      and parent_code = '11') bd on pi.payment_type_code = bd.dic_code
         left join (select data.`id`,
                           data.`order_id`,
                           data.`user_id`,
                           data.`province_id`
                    from ods_order_info_inc
                    where dt = '2024-05-05'
                      and type = 'bootstrap-insert') oi on od.order_id = oi.id
         left join (select data.`order_detail_id`,
                           data.`activity_id`,
                           data.`activity_rule_id`
                    from ods_order_detail_activity_inc
                    where dt = '2024-05-05'
                      and type = 'bootstrap-insert') oda on od.id = oda.order_detail_id
         left join (select data.`order_detail_id`,
                           data.`coupon_id`
                    from ods_order_detail_coupon_inc
                    where dt = '2024-05-05'
                      and type = 'bootstrap-insert') odc on od.id = odc.order_detail_id;


-- 装载数据（每日）
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt = '2024-05-06')
select od.`id`,
       od.`order_id`,
       `user_id`,
       `sku_id`,
       `province_id`,
       `activity_id`,
       `activity_rule_id`,
       `coupon_id`,
       `payment_type_code`,
       `payment_type_name`,
       `date_id`,
       `callback_time`,
       `sku_num`,
       `split_original_amount`,
       `split_activity_amount`,
       `split_coupon_amount`,
       `split_payment_amount`
from (select data.`id`,
             data.`order_id`,
             data.`sku_id`,
             data.`sku_num`,
             data.`sku_num` * data.`order_price` `split_original_amount`,
             data.`split_activity_amount`,
             data.`split_coupon_amount`
      from ods_order_detail_inc
      where
          /* 解决零点漂移问题 */
          (dt = '2024-05-06' or dt = date_sub('2024-05-06', 1))
        and (type = 'insert' or type = 'bootstrap-insert')) od
         join (select data.`order_id`,
                      data.`payment_type`                             `payment_type_code`,
                      data.`total_amount`                             `split_payment_amount`,
                      date_format(data.`callback_time`, 'yyyy-MM-dd') `date_id`,
                      data.`callback_time`
               from ods_payment_info_inc
               where dt = '2024-05-06'
                 and type = 'update'
                 and old['payment_status'] is not null
                 and data.payment_status = '1602') pi on od.id = pi.order_id
         left join (select dic_code,
                           dic_name `payment_type_name`
                    from ods_base_dic_full
                    where dt = '2024-05-06'
                      and parent_code = '11') bd on pi.payment_type_code = bd.dic_code
         left join (select data.`id`,
                           data.`order_id`,
                           data.`user_id`,
                           data.`province_id`
                    from ods_order_info_inc
                    where dt = '2024-05-06'
                      and type = 'insert') oi on od.order_id = oi.id
         left join (select data.`order_detail_id`,
                           data.`activity_id`,
                           data.`activity_rule_id`
                    from ods_order_detail_activity_inc
                    where dt = '2024-05-06'
                      and type = 'insert') oda on od.id = oda.order_detail_id
         left join (select data.`order_detail_id`,
                           data.`coupon_id`
                    from ods_order_detail_coupon_inc
                    where dt = '2024-05-06'
                      and type = 'insert') odc on od.id = odc.order_detail_id;