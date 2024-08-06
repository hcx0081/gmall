package com.gmall.realtime.common.util;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;

/**
 * 日期时间格式化工具类
 */
public class DateFormatUtils {
    public static String tsToDateString(Long ts) {
        return LocalDateTimeUtil.format(LocalDateTimeUtil.of(ts), DatePattern.NORM_DATE_PATTERN);
    }
    
    public static String tsToDateTimeString(Long ts) {
        return LocalDateTimeUtil.format(LocalDateTimeUtil.of(ts), DatePattern.NORM_DATETIME_PATTERN);
    }
}
