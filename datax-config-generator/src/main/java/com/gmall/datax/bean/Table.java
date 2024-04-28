package com.gmall.datax.bean;

import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class Table {
    private final String tableName;
    private final List<Column> columns;
    
    public Table(String tableName) {
        this.tableName = tableName;
        this.columns = new ArrayList<>();
    }
    
    public void addColumn(String name, String type) {
        columns.add(new Column(name, type));
    }
    
    public List<String> getColumnNames() {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }
    
    public List<Map<String, String>> getColumnNamesAndTypes() {
        List<Map<String, String>> result = new ArrayList<>();
        columns.forEach(column -> {
            Map<String, String> temp = new HashMap<>();
            temp.put("name", column.getName());
            temp.put("type", column.getHiveType());
            result.add(temp);
        });
        return result;
    }
}
