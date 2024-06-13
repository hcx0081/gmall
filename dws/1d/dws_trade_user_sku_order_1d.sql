-- 建表
drop table if exists dws_trade_user_sku_order_1d;
create external table dws_trade_user_sku_order_1d
(
    user_id                   string comment '用户id',
    sku_id                    string comment 'sku_id',
    sku_name                  string comment 'sku名称',
    category1_id              string comment '一级品类id',
    category1_name            string comment '一级品类名称',
    category2_id              string comment '二级品类id',
    category2_name            string comment '二级品类名称',
    category3_id              string comment '三级品类id',
    category3_name            string comment '三级品类名称',
    tm_id                     string comment '品牌id',
    tm_name                   string comment '品牌名称',
    order_count_1d            bigint comment '最近1日下单次数',
    order_num_1d              bigint comment '最近1日下单件数',
    order_original_amount_1d  decimal(16, 2) comment '最近1日下单原始金额',
    activity_reduce_amount_1d decimal(16, 2) comment '最近1日活动优惠金额',
    coupon_reduce_amount_1d   decimal(16, 2) comment '最近1日优惠券优惠金额',
    order_total_amount_1d     decimal(16, 2) comment '最近1日下单最终金额'
) comment '交易域用户商品粒度订单最近1日汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_trade_user_sku_order_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日：包含历史全部，所以使用动态分区）
insert overwrite table dws_trade_user_sku_order_1d partition (dt)
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
       count(distinct order_id)   order_count_1d,
       sum(sku_num)               order_num_1d,
       sum(split_original_amount) order_original_amount_1d,
       sum(split_activity_amount) activity_reduce_amount_1d,
       sum(split_coupon_amount)   coupon_reduce_amount_1d,
       sum(split_total_amount)    order_total_amount_1d,
       dt
from (select user_id,
             sku_id,
             order_id,
             sku_num,
             split_original_amount,
             split_activity_amount,
             split_coupon_amount,
             split_total_amount,
             dt
      from dwd_trade_order_detail_inc
         /* where dt = '2024-05-05' */) tod
         left join (select id,
                           sku_name,
                           category1_id,
                           category1_name,
                           category2_id,
                           category2_name,
                           category3_id,
                           category3_name,
                           tm_id,
                           tm_name
                    from dim_sku_full
                    where dt = '2024-05-05') sku on tod.sku_id = sku.id
group by dt,
         user_id,
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


-- 装载数据（每日）
insert overwrite table dws_trade_user_sku_order_1d partition (dt = '2024-05-06')
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
       count(distinct order_id)   order_count_1d,
       sum(sku_num)               order_num_1d,
       sum(split_original_amount) order_original_amount_1d,
       sum(split_activity_amount) activity_reduce_amount_1d,
       sum(split_coupon_amount)   coupon_reduce_amount_1d,
       sum(split_total_amount)    order_total_amount_1d
from (select user_id,
             sku_id,
             order_id,
             sku_num,
             split_original_amount,
             split_activity_amount,
             split_coupon_amount,
             split_total_amount
      from dwd_trade_order_detail_inc
      where dt = '2024-05-06') tod
         left join (select id,
                           sku_name,
                           category1_id,
                           category1_name,
                           category2_id,
                           category2_name,
                           category3_id,
                           category3_name,
                           tm_id,
                           tm_name
                    from dim_sku_full
                    where dt = '2024-05-06') sku on tod.sku_id = sku.id
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