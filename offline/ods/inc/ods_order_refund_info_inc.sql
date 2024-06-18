drop table if exists ods_order_refund_info_inc;
create external table ods_order_refund_info_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, user_id :string, order_id :string, sku_id :string, refund_type :string, refund_num
                  :bigint, refund_amount :decimal(16, 2), refund_reason_type :string, refund_reason_txt :string,
                  refund_status :string, create_time :string, operate_time :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '退单表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/gmall/warehouse/ods/ods_order_refund_info_inc/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');