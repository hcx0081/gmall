drop table if exists ods_base_province_full;
create external table ods_base_province_full
(
    `id`           string comment '编号',
    `name`         string comment '省份名称',
    `region_id`    string comment '地区id',
    `area_code`    string comment '地区编码',
    `iso_code`     string comment '旧版国际标准地区编码，供可视化使用',
    `iso_3166_2`   string comment '新版国际标准地区编码，供可视化使用',
    `create_time`  string comment '创建时间',
    `operate_time` string comment '修改时间'
) comment '省份表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_base_province_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');