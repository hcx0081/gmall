drop table if exists ods_order_detail_inc;
create external table ods_order_detail_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, order_id :string, sku_id :string, sku_name :string, img_url :string, order_price
                  :decimal(16, 2), sku_num :bigint, create_time :string, source_type :string, source_id :string,
                  split_total_amount :decimal(16, 2), split_activity_amount :decimal(16, 2), split_coupon_amount
                  :decimal(16, 2), operate_time :string> comment '数据',
    `old`  map<string, string> comment '旧值'
) comment '订单明细表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/gmall/warehouse/ods/ods_order_detail_inc/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');