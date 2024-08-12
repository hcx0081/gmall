package com.gmall.realtime.common.bean;


import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradeProvinceOrder {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 当天日期
    @JSONField(name = "cur_date")
    String curDate;
    
    // 省份 ID
    @JSONField(name = "province_id")
    String provinceId;
    // 省份名称
    @JSONField(name = "province_name")
    String provinceName;
    
    // 累计下单次数
    @JSONField(name = "order_count")
    Long orderCount;
    // 累计下单金额
    @JSONField(name = "order_amount")
    BigDecimal orderAmount;
    
    @JSONField(serialize = false)
    String orderDetailId;
    @JSONField(serialize = false)
    Set<String> orderIdSet;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
