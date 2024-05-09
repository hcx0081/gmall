drop table if exists ods_sku_info_full;
create
    external table ods_sku_info_full
(
    `id`              string comment 'sku_id',
    `spu_id`          string comment 'spu_id',
    `price`           decimal(16, 2) comment '价格',
    `sku_name`        string comment 'sku名称',
    `sku_desc`        string comment 'sku规格描述',
    `weight`          decimal(16, 2) comment '重量',
    `tm_id`           string comment '品牌id',
    `category3_id`    string comment '三级品类id',
    `sku_default_img` string comment '默认显示图片地址',
    `is_sale`         string comment '是否在售',
    `create_time`     string comment '创建时间',
    `operate_time`    string comment '修改时间'
) comment '商品表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_sku_info_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');