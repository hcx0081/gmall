drop table if exists ods_order_detail_coupon_inc;
create external table ods_order_detail_coupon_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, order_id :string, order_detail_id :string, coupon_id :string, coupon_use_id :string,
                  sku_id :string, create_time :string, operate_time :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '订单明细优惠券关联表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/gmall/warehouse/ods/ods_order_detail_coupon_inc/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');