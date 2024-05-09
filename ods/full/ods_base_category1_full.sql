drop table if exists ods_base_category1_full;
create external table ods_base_category1_full
(
    `id`           string comment '编号',
    `name`         string comment '分类名称',
    `create_time`  string comment '创建时间',
    `operate_time` string comment '修改时间'
) comment '一级品类表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_base_category1_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');