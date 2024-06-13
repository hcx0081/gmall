-- 建表
drop table if exists dws_tool_user_coupon_coupon_used_1d;
create external table dws_tool_user_coupon_coupon_used_1d
(
    user_id          string comment '用户id',
    coupon_id        string comment '优惠券id',
    coupon_name      string comment '优惠券名称',
    coupon_type_code string comment '优惠券类型编码',
    coupon_type_name string comment '优惠券类型名称',
    benefit_rule     string comment '优惠规则',
    used_count_1d    string comment '使用(支付)次数'
) comment '工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_tool_user_coupon_coupon_used_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition (dt)
select user_id,
       coupon_id,
       coupon_name,
       coupon_type_code,
       coupon_type_name,
       benefit_rule,
       used_count_1d,
       dt
from (select user_id,
             coupon_id,
             count(*) used_count_1d,
             dt
      from dwd_tool_coupon_used_inc
      group by dt, user_id, coupon_id) tcu
         left join (select id,
                           coupon_name,
                           coupon_type_code,
                           coupon_type_name,
                           benefit_rule
                    from dim_coupon_full
                    where dt = '2024-05-05') cou;


-- 装载数据（每日）
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition (dt = '2024-05-06')
select user_id,
       coupon_id,
       coupon_name,
       coupon_type_code,
       coupon_type_name,
       benefit_rule,
       used_count_1d
from (select user_id,
             coupon_id,
             count(*) used_count_1d
      from dwd_tool_coupon_used_inc
      where dt = '2024-05-06'
      group by user_id, coupon_id) tcu
         left join (select id,
                           coupon_name,
                           coupon_type_code,
                           coupon_type_name,
                           benefit_rule
                    from dim_coupon_full
                    where dt = '2024-05-06') cou;