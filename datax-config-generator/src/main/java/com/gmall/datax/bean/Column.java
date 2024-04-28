package com.gmall.datax.bean;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class Column {
    private static final Map<String, String> typeMap = new HashMap<>();
    
    static {
        typeMap.put("tinyint", "bigint");
        typeMap.put("smallint", "bigint");
        typeMap.put("int", "bigint");
        typeMap.put("bigint", "bigint");
        typeMap.put("float", "float");
        typeMap.put("double", "double");
    }
    
    private final String name;
    private final String type;
    private final String hiveType;
    
    public Column(String name, String type) {
        this.name = name;
        this.type = type;
        this.hiveType = typeMap.getOrDefault(type, "string");
    }
}
