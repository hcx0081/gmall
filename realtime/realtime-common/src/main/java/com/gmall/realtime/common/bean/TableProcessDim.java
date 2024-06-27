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
public class TableProcessDim {
    /**
     * 来源表名
     */
    private String sourceTable;
    /**
     * 目标表名
     */
    private String sinkTable;
    /**
     * 数据到HBase的列族
     */
    private String sinkFamily;
    /**
     * 输出字段
     */
    private String sinkColumns;
    /**
     * Sink到HBase的主键字段
     */
    private String sinkRowKey;
    /**
     * 配置表操作类型
     */
    private String op;
}
