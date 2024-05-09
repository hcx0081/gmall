drop table if exists ods_order_info_inc;
create external table ods_order_info_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, consignee :string, consignee_tel :string, total_amount :decimal(16, 2), order_status
                  :string, user_id :string, payment_way :string, delivery_address :string, order_comment :string,
                  out_trade_no :string, trade_body :string, create_time :string, operate_time :string, expire_time
                  :string, process_status :string, tracking_no :string, parent_order_id :string, img_url :string,
                  province_id :string, activity_reduce_amount :decimal(16, 2), coupon_reduce_amount :decimal(16, 2),
                  original_total_amount :decimal(16, 2), freight_fee :decimal(16, 2), freight_fee_reduce
                  :decimal(16, 2), refundable_time :decimal(16, 2)> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '订单表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/gmall/warehouse/ods/ods_order_info_inc/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');