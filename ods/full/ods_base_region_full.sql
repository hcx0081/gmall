drop table if exists ods_base_region_full;
create external table ods_base_region_full
(
    `id`           string comment '地区id',
    `region_name`  string comment '地区名称',
    `create_time`  string comment '创建时间',
    `operate_time` string comment '修改时间'
) comment '地区表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_base_region_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');