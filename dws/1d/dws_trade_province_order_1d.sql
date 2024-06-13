-- 建表
drop table if exists dws_trade_province_order_1d;
create external table dws_trade_province_order_1d
(
    province_id               string comment '省份id',
    province_name             string comment '省份名称',
    area_code                 string comment '地区编码',
    iso_code                  string comment '旧版国际标准地区编码',
    iso_3166_2                string comment '新版国际标准地区编码',
    order_count_1d            bigint comment '最近1日下单次数',
    order_original_amount_1d  decimal(16, 2) comment '最近1日下单原始金额',
    activity_reduce_amount_1d decimal(16, 2) comment '最近1日下单活动优惠金额',
    coupon_reduce_amount_1d   decimal(16, 2) comment '最近1日下单优惠券优惠金额',
    order_total_amount_1d     decimal(16, 2) comment '最近1日下单最终金额'
) comment '交易域省份粒度订单最近1日汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_trade_province_order_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dws_trade_province_order_1d partition (dt)
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d,
       dt
from (select province_id,
             count(distinct order_id)   order_count_1d,
             sum(split_original_amount) order_original_amount_1d,
             sum(split_activity_amount) activity_reduce_amount_1d,
             count(split_coupon_amount) coupon_reduce_amount_1d,
             count(split_total_amount)  order_total_amount_1d,
             dt
      from dwd_trade_order_detail_inc
      group by dt, province_id) tod
         left join (select id,
                           province_name,
                           area_code,
                           iso_code,
                           iso_3166_2
                    from dim_province_full
                    where dt = '2024-05-05') p
                   on tod.province_id = p.id;


-- 装载数据（每日）
insert overwrite table dws_trade_province_order_1d partition (dt = '2024-05-06')
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d
from (select province_id,
             count(distinct order_id)   order_count_1d,
             sum(split_original_amount) order_original_amount_1d,
             sum(split_activity_amount) activity_reduce_amount_1d,
             count(split_coupon_amount) coupon_reduce_amount_1d,
             count(split_total_amount)  order_total_amount_1d
      from dwd_trade_order_detail_inc
      where dt = '2024-05-06'
      group by province_id) tod
         left join (select id,
                           province_name,
                           area_code,
                           iso_code,
                           iso_3166_2
                    from dim_province_full
                    where dt = '2024-05-06') p
                   on tod.province_id = p.id;