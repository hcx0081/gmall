-- 建表
drop table if exists dws_trade_user_sku_order_nd;
create external table dws_trade_user_sku_order_nd
(
    user_id                    string comment '用户id',
    sku_id                     string comment 'sku_id',
    sku_name                   string comment 'sku名称',
    category1_id               string comment '一级品类id',
    category1_name             string comment '一级品类名称',
    category2_id               string comment '二级品类id',
    category2_name             string comment '二级品类名称',
    category3_id               string comment '三级品类id',
    category3_name             string comment '三级品类名称',
    tm_id                      string comment '品牌id',
    tm_name                    string comment '品牌名称',
    order_count_7d             string comment '最近7日下单次数',
    order_num_7d               bigint comment '最近7日下单件数',
    order_original_amount_7d   decimal(16, 2) comment '最近7日下单原始金额',
    activity_reduce_amount_7d  decimal(16, 2) comment '最近7日活动优惠金额',
    coupon_reduce_amount_7d    decimal(16, 2) comment '最近7日优惠券优惠金额',
    order_total_amount_7d      decimal(16, 2) comment '最近7日下单最终金额',
    order_count_30d            bigint comment '最近30日下单次数',
    order_num_30d              bigint comment '最近30日下单件数',
    order_original_amount_30d  decimal(16, 2) comment '最近30日下单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '最近30日活动优惠金额',
    coupon_reduce_amount_30d   decimal(16, 2) comment '最近30日优惠券优惠金额',
    order_total_amount_30d     decimal(16, 2) comment '最近30日下单最终金额'
) comment '交易域用户商品粒度订单最近n日汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_trade_user_sku_order_nd'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
insert overwrite table dws_trade_user_sku_order_nd partition (dt = '2024-05-05')
select user_id,
       sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       sum(if(dt >= date_sub('2024-05-05', 6), order_count_1d, 0))            order_count_7d,
       sum(order_count_1d)                                                    order_count_30d,
       sum(if(dt >= date_sub('2024-05-05', 6), order_num_1d, 0))              order_num_7d,
       sum(order_num_1d)                                                      order_num_30d,
       sum(if(dt >= date_sub('2024-05-05', 6), order_original_amount_1d, 0))  order_original_amount_7d,
       sum(order_original_amount_1d)                                          order_original_amount_30d,
       sum(if(dt >= date_sub('2024-05-05', 6), activity_reduce_amount_1d, 0)) activity_reduce_amount_7d,
       sum(activity_reduce_amount_1d)                                         activity_reduce_amount_30d,
       sum(if(dt >= date_sub('2024-05-05', 6), coupon_reduce_amount_1d, 0))   coupon_reduce_amount_7d,
       sum(coupon_reduce_amount_1d)                                           coupon_reduce_amount_30d,
       sum(if(dt >= date_sub('2024-05-05', 6), order_total_amount_1d, 0))     order_total_amount_7d,
       sum(order_total_amount_1d)                                             order_total_amount_30d
from dws_trade_user_sku_order_1d
where dt >= date_sub('2024-05-05', 29)
  and dt <= '2024-05-05'
group by user_id,
         sku_id,
         sku_name,
         category1_id,
         category1_name,
         category2_id,
         category2_name,
         category3_id,
         category3_name,
         tm_id,
         tm_name;