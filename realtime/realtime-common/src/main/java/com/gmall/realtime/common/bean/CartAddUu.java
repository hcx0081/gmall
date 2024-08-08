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
public class CartAddUu {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    @JSONField(name = "cur_date")
    String curDate;
    
    // 加购独立用户数
    @JSONField(name = "cart_add_uu_ct")
    Long cartAddUuCt;
}
