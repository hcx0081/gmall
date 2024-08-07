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
public class UserLogin {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    @JSONField(name = "cur_date")
    String curDate;
    
    // 回流用户数
    @JSONField(name = "back_ct")
    Long backCt;
    // 独立用户数
    @JSONField(name = "uu_ct")
    Long uuCt;
    
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
