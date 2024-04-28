package com.gmall.datax.helper;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.gmall.datax.Main;
import com.gmall.datax.bean.Table;
import com.gmall.datax.configuration.Configuration;
import lombok.Getter;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

@Getter
public class DataxJSONHelper {
    /* 解析 mySQL2HdfsConfig 和 hdfs2MySQLConfig 模板 */
    
    // Hadoop 单点集群
    private final JSONObject mySQL2HdfsConfig;
    private final JSONObject hdfs2MySQLConfig;
    
    {
        try {
            mySQL2HdfsConfig = JSONUtil.readJSONObject(
                    // Hadoop 单点集群
                    new File(Main.class.getClassLoader().getResource("mysql2hdfs_template_single.json").toURI()),
                    // Hadoop HA 集群
                    // new File(Main.class.getClassLoader().getResource("mysql2hdfs_template_ha.json").toURI()),
                    Charset.defaultCharset()
            );
            hdfs2MySQLConfig = JSONUtil.readJSONObject(
                    // Hadoop 单点集群
                    new File(Main.class.getClassLoader().getResource("hdfs2mysql_template_single.json").toURI()),
                    // Hadoop HA 集群
                    // new File(Main.class.getClassLoader().getResource("hdfs2mysql_template_ha.json").toURI()),
                    Charset.defaultCharset()
            );
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    
    public DataxJSONHelper() {
        // 获取Reader和Writer配置
        JSONObject mysqlReaderParameter = mySQL2HdfsConfig.getByPath("job.content[0].reader.parameter", JSONObject.class);
        JSONObject hdfsWriterParameter = mySQL2HdfsConfig.getByPath("job.content[0].writer.parameter", JSONObject.class);
        JSONObject hdfsReaderParameter = hdfs2MySQLConfig.getByPath("job.content[0].reader.parameter", JSONObject.class);
        JSONObject mySqlWriterParameter = hdfs2MySQLConfig.getByPath("job.content[0].writer.parameter", JSONObject.class);
        
        // 设置defaultFS
        hdfsReaderParameter.set("defaultFS", Configuration.HDFS_URI);
        hdfsWriterParameter.set("defaultFS", Configuration.HDFS_URI);
        
        // 设置MySQL username
        mysqlReaderParameter.set("username", Configuration.MYSQL_USER);
        mySqlWriterParameter.set("username", Configuration.MYSQL_USER);
        
        // 设置MySQL password
        mysqlReaderParameter.set("password", Configuration.MYSQL_PASSWORD);
        mySqlWriterParameter.set("password", Configuration.MYSQL_PASSWORD);
        
        // 设置MySQL JDBC URL
        mysqlReaderParameter.putByPath("connection[0].jdbcUrl[0]", Configuration.MYSQL_URL_IMPORT);
        mySqlWriterParameter.putByPath("connection[0].jdbcUrl", Configuration.MYSQL_URL_EXPORT);
        
        // 写回Reader和Writer配置
        mySQL2HdfsConfig.putByPath("job.content[0].reader.parameter", mysqlReaderParameter);
        mySQL2HdfsConfig.putByPath("job.content[0].writer.parameter", hdfsWriterParameter);
        hdfs2MySQLConfig.putByPath("job.content[0].reader.parameter", hdfsReaderParameter);
        hdfs2MySQLConfig.putByPath("job.content[0].writer.parameter", mySqlWriterParameter);
    }
    
    public void setTableAndColumns(Table table, int index, String migrationType) {
        // 设置表名
        setTable(table, index, migrationType);
        // 设置列名及路径
        setColumns(table, migrationType);
    }
    
    
    public void setTable(Table table, int index, String migrationType) {
        if (Configuration.MIGRATION_TYPE_MYSQL2HDFS.equals(migrationType)) {
            // 设置表名
            mySQL2HdfsConfig.putByPath("job.content[0].reader.parameter.connection[0].table[" + index + "]", table.getTableName());
        }
        if (Configuration.MIGRATION_TYPE_HDFS2MYSQL.equals(migrationType)) {
            // 设置表名
            hdfs2MySQLConfig.putByPath("job.content[0].writer.parameter.connection[0].table[" + index + "]", table.getTableName());
        }
    }
    
    public void setColumns(Table table, String migrationType) {
        if (Configuration.MIGRATION_TYPE_MYSQL2HDFS.equals(migrationType)) {
            // 设置HDFS Writer的fileName
            mySQL2HdfsConfig.putByPath("job.content[0].writer.parameter.fileName", table.getTableName());
            // 设置列名
            mySQL2HdfsConfig.putByPath("job.content[0].reader.parameter.column", table.getColumnNames());
            mySQL2HdfsConfig.putByPath("job.content[0].writer.parameter.column", table.getColumnNamesAndTypes());
        }
        if (Configuration.MIGRATION_TYPE_HDFS2MYSQL.equals(migrationType)) {
            // 设置列名
            hdfs2MySQLConfig.putByPath("job.content[0].writer.parameter.column", table.getColumnNames());
        }
    }
}
