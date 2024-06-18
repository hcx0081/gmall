-- 建表
drop table if exists dws_trade_province_order_nd;
create external table dws_trade_province_order_nd
(
    province_id                string comment '省份id',
    province_name              string comment '省份名称',
    area_code                  string comment '地区编码',
    iso_code                   string comment '旧版国际标准地区编码',
    iso_3166_2                 string comment '新版国际标准地区编码',
    order_count_7d             bigint comment '最近7日下单次数',
    order_original_amount_7d   decimal(16, 2) comment '最近7日下单原始金额',
    activity_reduce_amount_7d  decimal(16, 2) comment '最近7日下单活动优惠金额',
    coupon_reduce_amount_7d    decimal(16, 2) comment '最近7日下单优惠券优惠金额',
    order_total_amount_7d      decimal(16, 2) comment '最近7日下单最终金额',
    order_count_30d            bigint comment '最近30日下单次数',
    order_original_amount_30d  decimal(16, 2) comment '最近30日下单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '最近30日下单活动优惠金额',
    coupon_reduce_amount_30d   decimal(16, 2) comment '最近30日下单优惠券优惠金额',
    order_total_amount_30d     decimal(16, 2) comment '最近30日下单最终金额'
) comment '交易域省份粒度订单最近n日汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_trade_province_order_nd'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
insert overwrite table dws_trade_province_order_nd partition (dt = '2024-05-05')
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       sum(if(dt >= date_sub(order_count_1d, 6), order_count_1d, 0))                       order_count_7d,
       sum(if(dt >= date_sub(order_original_amount_1d, 6), order_original_amount_1d, 0))   order_original_amount_7d,
       sum(if(dt >= date_sub(activity_reduce_amount_1d, 6), activity_reduce_amount_1d, 0)) activity_reduce_amount_7d,
       sum(if(dt >= date_sub(coupon_reduce_amount_1d, 6), coupon_reduce_amount_1d, 0))     coupon_reduce_amount_7d,
       sum(if(dt >= date_sub(order_total_amount_1d, 6), order_total_amount_1d, 0))         order_total_amount_7d,
       sum(order_count_1d)                                                                 order_count_30d,
       sum(order_original_amount_1d)                                                       order_original_amount_30d,
       sum(activity_reduce_amount_1d)                                                      activity_reduce_amount_30d,
       sum(coupon_reduce_amount_1d)                                                        coupon_reduce_amount_30d,
       sum(order_total_amount_1d)                                                          order_total_amount_30d
from dws_trade_province_order_1d
where dt >= date_sub('2024-05-05', 29)
  and dt <= '2024-05-05'
group by province_id,
         province_name,
         area_code,
         iso_code,
         iso_3166_2;