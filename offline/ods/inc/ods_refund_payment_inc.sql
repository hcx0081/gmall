drop table if exists ods_refund_payment_inc;
create external table ods_refund_payment_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, out_trade_no :string, order_id :string, sku_id :string, payment_type :string, trade_no
                  :string, total_amount :decimal(16, 2), subject :string, refund_status :string, create_time :string,
                  callback_time :string, callback_content :string, operate_time :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '退款表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/gmall/warehouse/ods/ods_refund_payment_inc/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');