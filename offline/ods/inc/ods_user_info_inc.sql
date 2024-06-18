drop table if exists ods_user_info_inc;
create external table ods_user_info_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, login_name :string, nick_name :string, passwd :string, name :string, phone_num :string,
                  email :string, head_img :string, user_level :string, birthday :string, gender :string, create_time
                  :string, operate_time :string, status :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '用户表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/gmall/warehouse/ods/ods_user_info_inc/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');