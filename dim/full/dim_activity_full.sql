-- 建表
drop table if exists dim_activity_full;
create external table dim_activity_full
(
    `activity_rule_id`   string comment '活动规则id',
    `activity_id`        string comment '活动id',
    `activity_name`      string comment '活动名称',
    `activity_type_code` string comment '活动类型编码',
    `activity_type_name` string comment '活动类型名称',
    `activity_desc`      string comment '活动描述',
    `start_time`         string comment '开始时间',
    `end_time`           string comment '结束时间',
    `create_time`        string comment '创建时间',
    `condition_amount`   decimal(16, 2) comment '满减金额',
    `condition_num`      bigint comment '满减件数',
    `benefit_amount`     decimal(16, 2) comment '优惠金额',
    `benefit_discount`   decimal(16, 2) comment '优惠折扣',
    `benefit_rule`       string comment '优惠规则',
    `benefit_level`      string comment '优惠级别'
) comment '活动维度表'
    partitioned by (`dt` string)
    stored as orc
    location '/gmall/warehouse/dim/dim_activity_full/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
insert overwrite table dim_activity_full partition (dt = '2024-05-05')
select rule.id,
       info.id,
       activity_name,
       rule.activity_type,
       dic.dic_name,
       activity_desc,
       start_time,
       end_time,
       create_time,
       condition_amount,
       condition_num,
       benefit_amount,
       benefit_discount,
       case rule.activity_type
           when '3101' then concat('满', condition_amount, '元减', benefit_amount, '元')
           when '3102' then concat('满', condition_num, '件打', benefit_discount, ' 折')
           when '3103' then concat('打', benefit_discount, '折')
           end benefit_rule,
       benefit_level
from (select id,
             activity_id,
             activity_type,
             condition_amount,
             condition_num,
             benefit_amount,
             benefit_discount,
             benefit_level
      from ods_activity_rule_full
      where dt = '2024-05-05') rule
         left join
     (select id,
             activity_name,
             activity_type,
             activity_desc,
             start_time,
             end_time,
             create_time
      from ods_activity_info_full
      where dt = '2024-05-05') info
     on rule.activity_id = info.id
         left join
     (select dic_code,
             dic_name
      from ods_base_dic_full
      where dt = '2024-05-05'
        and parent_code = '31') dic
     on rule.activity_type = dic.dic_code;