drop table if exists ods_cart_info_full;
create external table ods_cart_info_full
(
    `id`           string comment '编号',
    `user_id`      string comment '用户id',
    `sku_id`       string comment 'sku_id',
    `cart_price`   decimal(16, 2) comment '放入购物车时价格',
    `sku_num`      bigint comment '数量',
    `img_url`      bigint comment '商品图片地址',
    `sku_name`     string comment 'sku名称 (冗余)',
    `is_checked`   string comment '是否被选中',
    `create_time`  string comment '创建时间',
    `operate_time` string comment '修改时间',
    `is_ordered`   string comment '是否已经下单',
    `order_time`   string comment '下单时间'
) comment '购物车全量表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_cart_info_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');