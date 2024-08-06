package com.gmall.realtime.common.bean;


import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TrafficHomeDetailPageView {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    
    // 首页独立访客数
    @JSONField(name = "home_uv_ct")
    Long homeUvCt;
    // 商品详情页独立访客数
    @JSONField(name = "good_detail_uv_ct")
    Long goodDetailUvCt;
    
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
    // 当天日期
    @JSONField(name = "cur_date")
    private String curDate;
}
