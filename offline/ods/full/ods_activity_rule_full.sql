drop table if exists ods_activity_rule_full;
create external table ods_activity_rule_full
(
    `id`               string comment '编号',
    `activity_id`      string comment '活动id',
    `activity_type`    string comment '活动类型',
    `condition_amount` decimal(16, 2) comment '满减金额',
    `condition_num`    bigint comment '满减件数',
    `benefit_amount`   decimal(16, 2) comment '优惠金额',
    `benefit_discount` decimal(16, 2) comment '优惠折扣',
    `benefit_level`    string comment '优惠级别',
    `create_time`      string comment '创建时间',
    `operate_time`     string comment '修改时间'
) comment '活动规则表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_activity_rule_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');