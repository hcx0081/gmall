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
public class TrafficPageView {
    // 窗口起始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // 当天日期
    @JSONField(name = "cur_date")
    private String curDate;
    
    // app版本号
    private String vc;
    // 渠道
    private String ch;
    // 地区
    private String ar;
    // 新老访客状态标记
    @JSONField(name = "is_new")
    private String isNew;
    
    // 独立访客数
    @JSONField(name = "uv_ct")
    private Long uvCt;
    // 会话数
    @JSONField(name = "sv_ct")
    private Long svCt;
    // 页面浏览数
    @JSONField(name = "pv_ct")
    private Long pvCt;
    // 累计访问时长
    @JSONField(name = "dur_sum")
    private Long durSum;
    
    // 时间戳
    @JSONField(serialize = false)
    private Long ts;
    @JSONField(serialize = false)
    private String sid;
}
