drop table if exists ods_coupon_use_inc;
create external table ods_coupon_use_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, coupon_id :string, user_id :string, order_id :string, coupon_status :string, get_time
                  :string, using_time :string, used_time :string,expire_time :string, create_time :string, operate_time
                  :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '优惠券领用表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/gmall/warehouse/ods/ods_coupon_use_inc/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');