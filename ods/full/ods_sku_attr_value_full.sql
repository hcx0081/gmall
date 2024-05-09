drop table if exists ods_sku_attr_value_full;
create external table ods_sku_attr_value_full
(
    `id`           string comment '编号',
    `attr_id`      string comment '平台属性id',
    `value_id`     string comment '平台属性值id',
    `sku_id`       string comment 'sku_id',
    `attr_name`    string comment '平台属性名称',
    `value_name`   string comment '平台属性值名称',
    `create_time`  string comment '创建时间',
    `operate_time` string comment '修改时间'
) comment '商品平台属性表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_sku_attr_value_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');