-- 建表
drop table if exists dim_sku_full;
create external table dim_sku_full
(
    `id`                   string comment 'sku_id',
    `price`                decimal(16, 2) comment '商品价格',
    `sku_name`             string comment '商品名称',
    `sku_desc`             string comment '商品描述',
    `weight`               decimal(16, 2) comment '重量',
    `is_sale`              boolean comment '是否在售',
    `spu_id`               string comment 'spu编号',
    `spu_name`             string comment 'spu名称',
    `category3_id`         string comment '三级品类id',
    `category3_name`       string comment '三级品类名称',
    `category2_id`         string comment '二级品类id',
    `category2_name`       string comment '二级品类名称',
    `category1_id`         string comment '一级品类id',
    `category1_name`       string comment '一级品类名称',
    `tm_id`                string comment '品牌id',
    `tm_name`              string comment '品牌名称',
    `sku_attr_values`      array<struct<attr_id :string, value_id :string, attr_name :string, value_name
                                        :string>> comment '平台属性',
    `sku_sale_attr_values` array<struct<sale_attr_id :string, sale_attr_value_id :string, sale_attr_name :string,
                                        sale_attr_value_name :string>> comment '销售属性',
    `create_time`          string comment '创建时间'
) comment '商品维度表'
    partitioned by (`dt` string)
    stored as orc
    location '/gmall/warehouse/dim/dim_sku_full/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
with sku as
         (select id,
                 price,
                 sku_name,
                 sku_desc,
                 weight,
                 is_sale,
                 spu_id,
                 category3_id,
                 tm_id,
                 create_time
          from ods_sku_info_full
          where dt = '2024-05-05'),
     spu as
         (select id,
                 spu_name
          from ods_spu_info_full
          where dt = '2024-05-05'),
     c3 as
         (select id,
                 name,
                 category2_id
          from ods_base_category3_full
          where dt = '2024-05-05'),
     c2 as
         (select id,
                 name,
                 category1_id
          from ods_base_category2_full
          where dt = '2024-05-05'),
     c1 as
         (select id,
                 name
          from ods_base_category1_full
          where dt = '2024-05-05'),
     tm as
         (select id,
                 tm_name
          from ods_base_trademark_full
          where dt = '2024-05-05'),
     attr as
         (select sku_id,
                 collect_set(named_struct('attr_id', attr_id, 'value_id', value_id, 'attr_name', attr_name,
                                          'value_name', value_name)) attrs
          from ods_sku_attr_value_full
          where dt = '2024-05-05'
          group by sku_id),
     sale_attr as
         (select sku_id,
                 collect_set(named_struct('sale_attr_id', sale_attr_id, 'sale_attr_value_id', sale_attr_value_id,
                                          'sale_attr_name', sale_attr_name, 'sale_attr_value_name',
                                          sale_attr_value_name)) sale_attrs
          from ods_sku_sale_attr_value_full
          where dt = '2024-05-05'
          group by sku_id)
insert
overwrite
table
dim_sku_full
partition
(
dt = '2024-05-05'
)
select sku.id,
       sku.price,
       sku.sku_name,
       sku.sku_desc,
       sku.weight,
       sku.is_sale,
       sku.spu_id,
       spu.spu_name,
       sku.category3_id,
       c3.name,
       c3.category2_id,
       c2.name,
       c2.category1_id,
       c1.name,
       sku.tm_id,
       tm.tm_name,
       attr.attrs,
       sale_attr.sale_attrs,
       sku.create_time
from sku
         left join spu on sku.spu_id = spu.id
         left join c3 on sku.category3_id = c3.id
         left join c2 on c3.category2_id = c2.id
         left join c1 on c2.category1_id = c1.id
         left join tm on sku.tm_id = tm.id
         left join attr on sku.id = attr.sku_id
         left join sale_attr on sku.id = sale_attr.sku_id;