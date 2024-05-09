drop table if exists ods_order_detail_activity_inc;
create external table ods_order_detail_activity_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, order_id :string, order_detail_id :string, activity_id :string, activity_rule_id :string,
                  sku_id :string, create_time :string, operate_time :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '订单明细活动关联表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/gmall/warehouse/ods/ods_order_detail_activity_inc/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');