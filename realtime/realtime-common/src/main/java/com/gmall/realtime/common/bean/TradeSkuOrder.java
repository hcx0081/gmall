package com.gmall.realtime.common.bean;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeSkuOrder {
    @JSONField(serialize = false)
    String orderDetailId;
    
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 当天日期
    @JSONField(name = "cur_date")
    String curDate;
    
    // 品牌ID
    @JSONField(name = "trademark_id")
    String trademarkId;
    // 品牌名称
    @JSONField(name = "trademark_name")
    String trademarkName;
    // 一级品类ID
    @JSONField(name = "category1_id")
    String category1Id;
    // 一级品类名称
    @JSONField(name = "category1_name")
    String category1Name;
    // 二级品类 ID
    @JSONField(name = "category2_id")
    String category2Id;
    // 二级品类名称
    @JSONField(name = "category2_name")
    String category2Name;
    // 三级品类ID
    @JSONField(name = "category3_id")
    String category3Id;
    // 三级品类名称
    @JSONField(name = "category3_name")
    String category3Name;
    // sku_id
    @JSONField(name = "sku_id")
    String skuId;
    // sku名称
    @JSONField(name = "sku_name")
    String skuName;
    // spu_id
    @JSONField(name = "spu_id")
    String spuId;
    // spu名称
    @JSONField(name = "spu_name")
    String spuName;
    
    // 原始金额
    @JSONField(name = "original_amount")
    BigDecimal originalAmount;
    // 活动减免金额
    @JSONField(name = "activity_reduce_amount")
    BigDecimal activityReduceAmount;
    // 优惠券减免金额
    @JSONField(name = "coupon_reduce_amount")
    BigDecimal couponReduceAmount;
    // 下单金额
    @JSONField(name = "order_amount")
    BigDecimal orderAmount;
    
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
