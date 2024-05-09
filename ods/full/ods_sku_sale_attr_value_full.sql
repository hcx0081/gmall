drop table if exists ods_sku_sale_attr_value_full;
create
    external table ods_sku_sale_attr_value_full
(
    `id`                   string comment '编号',
    `sku_id`               string comment 'sku_id',
    `spu_id`               string comment 'spu_id',
    `sale_attr_value_id`   string comment '销售属性值id',
    `sale_attr_id`         string comment '销售属性id',
    `sale_attr_name`       string comment '销售属性名称',
    `sale_attr_value_name` string comment '销售属性值名称',
    `create_time`          string comment '创建时间',
    `operate_time`         string comment '修改时间'
) comment '商品销售属性值表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_sku_sale_attr_value_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');