package com.gmall.realtime.common.util;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;

import java.time.ZoneOffset;

/**
 * 日期时间格式化工具类
 */
public class DateFormatUtils {
    public static String tsToDateString(Long ts) {
        return LocalDateTimeUtil.format(LocalDateTimeUtil.of(ts), DatePattern.NORM_DATE_PATTERN);
    }
    
    public static Long dateStringToTs(String date) {
        return LocalDateTimeUtil.parse(date, DatePattern.NORM_DATE_PATTERN).toEpochSecond(ZoneOffset.UTC);
    }
    
    public static String tsToDateTimeString(Long ts) {
        return LocalDateTimeUtil.format(LocalDateTimeUtil.of(ts), DatePattern.NORM_DATETIME_PATTERN);
    }
}
