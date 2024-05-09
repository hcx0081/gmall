drop table if exists ods_coupon_info_full;
create external table ods_coupon_info_full
(
    `id`               string comment '购物券编号',
    `coupon_name`      string comment '购物券名称',
    `coupon_type`      string comment '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` decimal(16, 2) comment '满额数',
    `condition_num`    bigint comment '满件数',
    `activity_id`      string comment '活动编号',
    `benefit_amount`   decimal(16, 2) comment '减免金额',
    `benefit_discount` decimal(16, 2) comment '折扣',
    `create_time`      string comment '创建时间',
    `range_type`       string comment '范围类型 1、商品(spuid) 2、品类(三级品类id) 3、品牌',
    `limit_num`        bigint comment '最多领用次数',
    `taken_count`      bigint comment '已领用次数',
    `start_time`       string comment '可以领取的开始时间',
    `end_time`         string comment '可以领取的结束时间',
    `operate_time`     string comment '修改时间',
    `expire_time`      string comment '过期时间',
    `range_desc`       string comment '范围描述'
) comment '优惠券信息表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_coupon_info_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');