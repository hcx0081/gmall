-- 建表
drop table if exists ods_log_inc;
create external table ods_log_inc
(
    `common`   struct<ar :string, ba :string, ch :string, is_new :string, md :string, mid :string, os :string, sid
                      :string, uid :string, vc :string> comment '公共信息',
    `page`     struct<during_time :string, item :string, item_type :string, last_page_id :string, page_id :string,
                      from_pos_id :string, from_pos_seq :string, refer_id :string> comment '页面信息',
    `actions`  array<struct<action_id :string, item :string, item_type :string, ts :bigint>> comment '动作信息',
    `displays` array<struct<display_type :string, item :string, item_type :string, `pos_seq` :string, pos_id
                            :string>> comment '曝光信息',
    `start`    struct<entry :string, first_open :bigint, loading_time :bigint, open_ad_id :bigint, open_ad_ms :bigint,
                      open_ad_skip_ms :bigint> comment '启动信息',
    `err`      struct<error_code :bigint, msg :string> comment '错误信息',
    `ts`       bigint comment '时间戳'
) comment '活动信息表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/gmall/warehouse/ods/ods_log_inc/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');


-- 装载数据
load data inpath '/gmall/od/log/topic_log/2024-05-05' into table ods_log_inc partition (dt = '2024-05-05');