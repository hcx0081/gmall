drop table if exists ods_cart_info_inc;
create external table ods_cart_info_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, user_id :string, sku_id :string, cart_price :decimal(16, 2), sku_num :bigint, img_url
                  :string, sku_name :string, is_checked :string, create_time :string, operate_time :string, is_ordered
                  :string, order_time :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '购物车增量表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/gmall/warehouse/ods/ods_cart_info_inc/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');