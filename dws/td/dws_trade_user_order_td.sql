-- 建表
drop table if exists dws_trade_user_order_td;
create external table dws_trade_user_order_td
(
    user_id                   string comment '用户id',
    order_date_first          string comment '历史至今首次下单日期',
    order_date_last           string comment '历史至今末次下单日期',
    order_count_td            bigint comment '历史至今下单次数',
    order_num_td              bigint comment '历史至今购买商品件数',
    original_amount_td        decimal(16, 2) comment '历史至今下单原始金额',
    activity_reduce_amount_td decimal(16, 2) comment '历史至今下单活动优惠金额',
    coupon_reduce_amount_td   decimal(16, 2) comment '历史至今下单优惠券优惠金额',
    total_amount_td           decimal(16, 2) comment '历史至今下单最终金额'
) comment '交易域用户粒度订单历史至今汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_trade_user_order_td'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dws_trade_user_order_td partition (dt = '2024-05-05')
select user_id,
       min(dt)                        order_date_first,
       max(dt)                        order_date_last,
       sum(order_count_1d)            order_count_td,
       sum(order_num_1d)              order_num_td,
       sum(order_original_amount_1d)  original_amount_td,
       sum(activity_reduce_amount_1d) activity_reduce_amount_td,
       sum(coupon_reduce_amount_1d)   coupon_reduce_amount_td,
       sum(order_total_amount_1d)     total_amount_td
from dws_trade_user_order_1d
group by user_id;


-- 装载数据（每日）
insert overwrite table dws_trade_user_order_td partition (dt = '2024-05-06')
select user_id,
       min(order_date_first),
       max(order_date_last),
       sum(order_count_td),
       sum(order_num_td),
       sum(original_amount_td),
       sum(activity_reduce_amount_td),
       sum(coupon_reduce_amount_td),
       sum(total_amount_td)
from (select user_id,
             order_date_first,
             order_date_last,
             order_count_td,
             order_num_td,
             original_amount_td,
             activity_reduce_amount_td,
             coupon_reduce_amount_td,
             total_amount_td
      from dws_trade_user_order_td
      where dt = date_sub('2024-05-06', 1)
      union
      select user_id,
             '2024-05-06' order_date_first,
             '2024-05-06' order_date_last,
             order_count_1d,
             order_num_1d,
             order_original_amount_1d,
             activity_reduce_amount_1d,
             coupon_reduce_amount_1d,
             order_total_amount_1d
      from dws_trade_user_order_1d
      where dt = '2024-05-06') t
group by user_id;
