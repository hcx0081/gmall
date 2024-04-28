package com.gmall.datax.util;

import cn.hutool.json.JSON;

import java.io.Writer;

public class JSONUtil extends cn.hutool.json.JSONUtil {
    public static void toJsonPrettyStr(JSON json, Writer writer) {
        if (null != json) {
            json.write(writer, 4, 0);
        }
    }
}
