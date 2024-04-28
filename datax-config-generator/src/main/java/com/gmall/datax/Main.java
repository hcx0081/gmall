package com.gmall.datax;

import com.gmall.datax.bean.Table;
import com.gmall.datax.configuration.Configuration;
import com.gmall.datax.helper.DataxJSONHelper;
import com.gmall.datax.helper.MySQLHelper;
import com.gmall.datax.util.JSONUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        // 生成MySQL -> HDFS的配置文件
        if (Configuration.OUT_DIR_MYSQL2HDFS != null && !"".equals(Configuration.OUT_DIR_MYSQL2HDFS)) {
            MySQLHelper mySQLHelper = new MySQLHelper(
                    Configuration.MYSQL_URL_MYSQL2HDFS,
                    Configuration.MYSQL_DATABASE_MYSQL2HDFS,
                    Configuration.MYSQL_TABLES_MYSQL2HDFS
            );
            DataxJSONHelper dataxJSONHelper = new DataxJSONHelper();
            
            // 获取迁移操作类型
            String migrationType = Configuration.MIGRATION_TYPE_MYSQL2HDFS;
            
            // 创建父文件夹
            Files.createDirectories(Paths.get(Configuration.OUT_DIR_MYSQL2HDFS));
            List<Table> tables = mySQLHelper.getTables();
            
            // 是否分表生成，根据配置采用不同的处理策略
            if ("1".equals(Configuration.IS_SEPERATED_TABLES)) {
                for (int i = 0; i < tables.size(); i++) {
                    Table table = tables.get(i);
                    dataxJSONHelper.setTable(table, i, migrationType);
                }
                dataxJSONHelper.setColumns(tables.get(0), migrationType);
                
                // 输出最终JSON配置
                FileWriter inputWriter = new FileWriter(
                        Configuration.OUT_DIR_MYSQL2HDFS + "/" +
                                "mysql2hdfs_" +
                                Configuration.MYSQL_DATABASE_MYSQL2HDFS + "." +
                                tables.get(0).getTableName() + ".json"
                );
                JSONUtil.toJsonPrettyStr(dataxJSONHelper.getMySQL2HdfsConfig(), inputWriter);
                inputWriter.close();
            } else {
                for (Table table : tables) {
                    // 设置表信息
                    dataxJSONHelper.setTableAndColumns(table, 0, migrationType);
                    
                    // 输出最终JSON配置文件
                    FileWriter inputWriter = new FileWriter(
                            Configuration.OUT_DIR_MYSQL2HDFS + "/" +
                                    "mysql2hdfs_" +
                                    Configuration.MYSQL_DATABASE_MYSQL2HDFS + "." +
                                    table.getTableName() + ".json"
                    );
                    JSONUtil.toJsonPrettyStr(dataxJSONHelper.getMySQL2HdfsConfig(), inputWriter);
                    inputWriter.close();
                }
            }
        }
        
        // 生成HDFS -> MySQL的配置文件
        if (Configuration.OUT_DIR_HDFS2MYSQL != null && !"".equals(Configuration.OUT_DIR_HDFS2MYSQL)) {
            MySQLHelper mysqlHelper = new MySQLHelper(
                    Configuration.MYSQL_URL_HDFS2MYSQL,
                    Configuration.MYSQL_DATABASE_HDFS2MYSQL,
                    Configuration.MYSQL_TABLES_HDFS2MYSQL
            );
            DataxJSONHelper dataxJsonHelper = new DataxJSONHelper();
            
            // 获取迁移操作类型
            String migrationType = Configuration.MIGRATION_TYPE_HDFS2MYSQL;
            
            // 创建父文件夹
            Files.createDirectories(Paths.get(Configuration.OUT_DIR_HDFS2MYSQL));
            List<Table> tables = mysqlHelper.getTables();
            
            // 是否分表生成，根据配置采用不同的处理策略
            if ("1".equals(Configuration.IS_SEPERATED_TABLES)) {
                for (int i = 0; i < tables.size(); i++) {
                    Table table = tables.get(i);
                    dataxJsonHelper.setTable(table, i, migrationType);
                }
                dataxJsonHelper.setColumns(tables.get(0), migrationType);
                
                // 输出最终JSON配置
                FileWriter outputWriter = new FileWriter(
                        Configuration.OUT_DIR_HDFS2MYSQL + "/" +
                                "hdfs2mysql_" +
                                Configuration.MYSQL_DATABASE_HDFS2MYSQL + "." +
                                tables.get(0).getTableName() + ".json"
                );
                JSONUtil.toJsonPrettyStr(dataxJsonHelper.getHdfs2MySQLConfig(), outputWriter);
                outputWriter.close();
            } else {
                for (Table table : tables) {
                    // 设置表信息
                    dataxJsonHelper.setTableAndColumns(table, 0, migrationType);
                    
                    // 输出最终JSON配置文件
                    FileWriter outputWriter = new FileWriter(
                            Configuration.OUT_DIR_HDFS2MYSQL + "/" +
                                    "hdfs2mysql_" +
                                    Configuration.MYSQL_DATABASE_HDFS2MYSQL + "." +
                                    table.getTableName() + ".json"
                    );
                    JSONUtil.toJsonPrettyStr(dataxJsonHelper.getHdfs2MySQLConfig(), outputWriter);
                    outputWriter.close();
                }
            }
        }
    }
}
