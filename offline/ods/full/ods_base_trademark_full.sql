drop table if exists ods_base_trademark_full;
create external table ods_base_trademark_full
(
    `id`           string comment '编号',
    `tm_name`      string comment '品牌名称',
    `logo_url`     string comment '品牌logo的图片路径',
    `create_time`  string comment '创建时间',
    `operate_time` string comment '修改时间'
) comment '品牌表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_base_trademark_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');