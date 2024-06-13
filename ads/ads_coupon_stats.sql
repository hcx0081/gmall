-- 建表
drop table if exists ads_coupon_stats;
create external table ads_coupon_stats
(
    dt              string comment '统计日期',
    coupon_id       string comment '优惠券id',
    coupon_name     string comment '优惠券名称',
    used_count      bigint comment '使用次数',
    used_user_count bigint comment '使用人数'
) comment '优惠券使用统计'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_coupon_stats/';


-- 装载数据
insert overwrite table ads_coupon_stats
select *
from ads_coupon_stats
union
select '2024-05-05',
       coupon_id,
       coupon_name,
       sum(used_count_1d) used_count,
       count(user_id)     used_user_count
from dws_tool_user_coupon_coupon_used_1d
where dt = '2024-05-05'
group by coupon_id,
         coupon_name;