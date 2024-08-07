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
public class UserRegister {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    @JSONField(name = "cur_date")
    String curDate;
    
    // 注册用户数
    @JSONField(name = "register_ct")
    Long registerCt;
    
    // 创建时间
    @JSONField(serialize = false)
    String createTime;
}
