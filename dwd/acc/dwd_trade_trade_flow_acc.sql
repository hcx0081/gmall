-- 建表
drop table if exists dwd_trade_trade_flow_acc;
create external table dwd_trade_trade_flow_acc
(
    order_id              string comment '订单id',
    user_id               string comment '用户id',
    province_id           string comment '省份id',
    order_date_id         string comment '下单日期id',
    order_time            string comment '下单时间',
    payment_date_id       string comment '支付日期id',
    payment_time          string comment '支付时间',
    finish_date_id        string comment '确认收货日期id',
    finish_time           string comment '确认收货时间',
    order_original_amount decimal(16, 2) comment '下单原始价格',
    order_activity_amount decimal(16, 2) comment '下单活动优惠分摊',
    order_coupon_amount   decimal(16, 2) comment '下单优惠券优惠分摊',
    order_total_amount    decimal(16, 2) comment '下单最终价格分摊',
    payment_amount        decimal(16, 2) comment '支付金额'
) comment '交易域交易流程累积快照事实表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dwd/dwd_trade_trade_flow_acc/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dwd_trade_trade_flow_acc partition (dt)
select oi.order_id,
       user_id,
       province_id,
       order_date_id,
       order_time,
       payment_date_id,
       payment_time,
       finish_date_id,
       finish_time,
       order_original_amount,
       order_activity_amount,
       order_coupon_amount,
       order_total_amount,
       payment_amount,
       nvl(finish_date_id, '9999-12-31')
from (select data.id                                     order_id,
             data.user_id,
             data.province_id,
             date_format(data.create_time, 'yyyy-MM-dd') order_date_id,
             data.create_time                            order_time,
             data.original_total_amount                  order_original_amount,
             data.activity_reduce_amount                 order_activity_amount,
             data.coupon_reduce_amount                   order_coupon_amount,
             data.total_amount                           order_total_amount
      from ods_order_info_inc
      where dt = '2024-05-05'
        and type = 'bootstrap-insert') oi
         left join (select data.order_id,
                           date_format(data.callback_time, 'yyyy-MM-dd') payment_date_id,
                           data.callback_time                            payment_time,
                           data.total_amount                             payment_amount
                    from ods_payment_info_inc
                    where dt = '2024-05-05'
                      and type = 'bootstrap-insert'
                      and data.payment_status = '1602') pi on oi.order_id = pi.order_id
         left join (select data.order_id,
                           date_format(data.create_time, 'yyyy-MM-dd') finish_date_id,
                           data.create_time                            finish_time
                    from ods_order_status_log_inc
                    where dt = '2024-05-05'
                      and type = 'bootstrap-insert'
                      and data.order_status = '1004') osl on oi.order_id = osl.order_id;


-- 装载数据（每日）
insert overwrite table dwd_trade_trade_flow_acc partition (dt)
select oi.order_id,
       user_id,
       province_id,
       order_date_id,
       order_time,
       if(pi.payment_date_id is not null, pi.payment_date_id, oi.payment_date_id),
       if(pi.payment_time is not null, pi.payment_time, oi.payment_time),
       if(osl.finish_date_id is not null, osl.finish_date_id, null),
       if(osl.finish_time is not null, osl.finish_time, null),
       order_original_amount,
       order_activity_amount,
       order_coupon_amount,
       order_total_amount,
       if(pi.payment_amount is not null, pi.payment_amount, oi.payment_amount),
       if(osl.finish_date_id is not null, osl.finish_date_id, '9999-12-31')
from (select order_id,
             user_id,
             province_id,
             order_date_id,
             order_time,
             payment_date_id,
             payment_time,
             finish_date_id,
             finish_time,
             order_original_amount,
             order_activity_amount,
             order_coupon_amount,
             order_total_amount,
             payment_amount
      from dwd_trade_trade_flow_acc
      where dt = '9999-12-31'
      union all
      select data.id                                     order_id,
             data.user_id,
             data.province_id,
             date_format(data.create_time, 'yyyy-MM-dd') order_date_id,
             data.create_time                            order_time,
             null,
             null,
             null,
             null,
             data.original_total_amount                  order_original_amount,
             data.activity_reduce_amount                 order_activity_amount,
             data.coupon_reduce_amount                   order_coupon_amount,
             data.total_amount                           order_total_amount,
             null
      from ods_order_info_inc
      where dt = '2024-05-06'
        and type = 'insert') oi
         left join (select data.order_id,
                           date_format(data.callback_time, 'yyyy-MM-dd') payment_date_id,
                           data.callback_time                            payment_time,
                           data.total_amount                             payment_amount
                    from ods_payment_info_inc
                    where dt = '2024-05-06'
                      and type = 'update'
                      and old['payment_status'] is not null
                      and data.payment_status = '1602') pi on oi.order_id = pi.order_id
         left join (select data.order_id,
                           date_format(data.create_time, 'yyyy-MM-dd') finish_date_id,
                           data.create_time                            finish_time
                    from ods_order_status_log_inc
                    where dt = '2024-05-06'
                      and data.order_status = '1004') osl on oi.order_id = osl.order_id;