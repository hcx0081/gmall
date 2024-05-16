-- 建表
drop table if exists dim_coupon_full;
create external table dim_coupon_full
(
    `id`               string comment '优惠券编号',
    `coupon_name`      string comment '优惠券名称',
    `coupon_type_code` string comment '优惠券类型编码',
    `coupon_type_name` string comment '优惠券类型名称',
    `condition_amount` decimal(16, 2) comment '满额数',
    `condition_num`    bigint comment '满件数',
    `activity_id`      string comment '活动编号',
    `benefit_amount`   decimal(16, 2) comment '减免金额',
    `benefit_discount` decimal(16, 2) comment '折扣',
    `benefit_rule`     string comment '优惠规则:满元*减*元，满*件打*折',
    `create_time`      string comment '创建时间',
    `range_type_code`  string comment '优惠范围类型编码',
    `range_type_name`  string comment '优惠范围类型名称',
    `limit_num`        bigint comment '最多领取次数',
    `taken_count`      bigint comment '已领取次数',
    `start_time`       string comment '可以领取的开始时间',
    `end_time`         string comment '可以领取的结束时间',
    `operate_time`     string comment '修改时间',
    `expire_time`      string comment '过期时间'
) comment '优惠券维度表'
    partitioned by (`dt` string)
    stored as orc
    location '/gmall/warehouse/dim/dim_coupon_full/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
insert overwrite table dim_coupon_full partition (dt = '2024-05-05')
select id,
       coupon_name,
       coupon_type,
       coupon_dic.dic_name,
       condition_amount,
       condition_num,
       activity_id,
       benefit_amount,
       benefit_discount,
       case coupon_type
           when '3201' then concat('满', condition_amount, '元减', benefit_amount, '元')
           when '3202' then concat('满', condition_num, '件打', benefit_discount, '折')
           when '3203' then concat('减', benefit_amount, '元')
           end benefit_rule,
       create_time,
       range_type,
       range_dic.dic_name,
       limit_num,
       taken_count,
       start_time,
       end_time,
       operate_time,
       expire_time
from (select id,
             coupon_name,
             coupon_type,
             condition_amount,
             condition_num,
             activity_id,
             benefit_amount,
             benefit_discount,
             create_time,
             range_type,
             limit_num,
             taken_count,
             start_time,
             end_time,
             operate_time,
             expire_time
      from ods_coupon_info_full
      where dt = '2024-05-05') ci
         left join
     (select dic_code,
             dic_name
      from ods_base_dic_full
      where dt = '2024-05-05'
        and parent_code = '32') coupon_dic
     on ci.coupon_type = coupon_dic.dic_code
         left join
     (select dic_code,
             dic_name
      from ods_base_dic_full
      where dt = '2024-05-05'
        and parent_code = '33') range_dic
     on ci.range_type = range_dic.dic_code;