drop table if exists ods_promotion_pos_full;
create external table ods_promotion_pos_full
(
    `id`             string comment '营销坑位id',
    `pos_location`   string comment '营销坑位位置',
    `pos_type`       string comment '营销坑位类型：banner,宫格,列表,瀑布',
    `promotion_type` string comment '营销类型：算法、固定、搜索',
    `create_time`    string comment '创建时间',
    `operate_time`   string comment '修改时间'
) comment '营销坑位表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_promotion_pos_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');