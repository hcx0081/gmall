package com.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 配置表Bean
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDwd {
    /**
     * 来源表名
     */
    private String sourceTable;
    
    /**
     * 来源操作类型
     */
    private String sourceType;
    
    /**
     * 目标表名
     */
    private String sinkTable;
    
    /**
     * 输出字段
     */
    private String sinkColumns;
    
    /**
     * 配置表操作类型
     */
    private String op;
}
