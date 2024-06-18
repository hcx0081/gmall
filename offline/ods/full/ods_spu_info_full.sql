drop table if exists ods_spu_info_full;
create external table ods_spu_info_full
(
    `id`           string comment 'spu_id',
    `spu_name`     string comment 'spu名称',
    `description`  string comment '描述信息',
    `category3_id` string comment '三级品类id',
    `tm_id`        string comment '品牌id',
    `create_time`  string comment '创建时间',
    `operate_time` string comment '修改时间'
) comment 'spu表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_spu_info_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');