drop table if exists ods_activity_info_full;
create external table ods_activity_info_full
(
    `id`            string comment '活动id',
    `activity_name` string comment '活动名称',
    `activity_type` string comment '活动类型',
    `activity_desc` string comment '活动描述',
    `start_time`    string comment '开始时间',
    `end_time`      string comment '结束时间',
    `create_time`   string comment '创建时间',
    `operate_time`  string comment '修改时间'
) comment '活动信息表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_activity_info_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');