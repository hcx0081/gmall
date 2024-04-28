package com.gmall.datax.helper;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.DSFactory;
import cn.hutool.setting.Setting;
import com.gmall.datax.bean.Table;
import com.gmall.datax.configuration.Configuration;
import lombok.Getter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Getter
public class MySQLHelper {
    private final List<Table> tables;
    
    public MySQLHelper(String url, String database, String mySQLTables) {
        tables = new ArrayList<>();
        
        Db db = Db.use(DSFactory.create(
                Setting.create()
                       .set("url", url)
                       .set("user", Configuration.MYSQL_USER)
                       .set("pass", Configuration.MYSQL_PASSWORD)
                       .set("showSql", "true")
                       .set("showParams", "true")
                       .set("sqlLevel", "info")
        ).getDataSource());
        
        // 获取设置的表，如未设置，查询数据库下面的所有表
        if (mySQLTables != null && !"".equals(mySQLTables)) {
            for (String mysqlTable : mySQLTables.split(",")) {
                tables.add(new Table(mysqlTable));
            }
        } else {
            try {
                db.findAll(Entity.create("information_schema.TABLES")
                                 .set("TABLE_SCHEMA", database))
                  .forEach(entity -> tables.add(new Table(entity.getStr("TABLE_NAME"))));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        
        // 获取所有表的列
        for (Table table : tables) {
            try {
                db.findAll(Entity.create("information_schema.COLUMNS")
                                 .set("TABLE_SCHEMA", database)
                                 .set("TABLE_NAME", table.getTableName()))
                  .stream()
                  .sorted(Comparator.comparingInt(o -> o.getInt("ORDINAL_POSITION")))
                  .forEach(entity -> table.addColumn(
                          entity.getStr("COLUMN_NAME"),
                          entity.getStr("DATA_TYPE")
                  ));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
