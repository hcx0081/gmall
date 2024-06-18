drop table if exists ods_promotion_refer_full;
create external table ods_promotion_refer_full
(
    `id`           string comment '外部营销渠道id',
    `refer_name`   string comment '外部营销渠道名称',
    `create_time`  string comment '创建时间',
    `operate_time` string comment '修改时间'
) comment '营销渠道表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_promotion_refer_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');