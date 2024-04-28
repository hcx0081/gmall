package com.gmall.datax.configuration;

import com.gmall.datax.Main;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class Configuration {
    public static String MYSQL_HOST;
    public static String MYSQL_PORT;
    public static String MYSQL_USER;
    public static String MYSQL_PASSWORD;
    public static String MYSQL_DATABASE_MYSQL2HDFS;
    public static String MYSQL_DATABASE_HDFS2MYSQL;
    public static String MYSQL_URL_IMPORT;
    public static String MYSQL_URL_EXPORT;
    public static String MYSQL_TABLES_MYSQL2HDFS;
    public static String MYSQL_TABLES_HDFS2MYSQL;
    public static String IS_SEPERATED_TABLES;
    public static String HDFS_URI;
    public static String OUT_DIR_MYSQL2HDFS;
    public static String OUT_DIR_HDFS2MYSQL;
    public static String MIGRATION_TYPE_MYSQL2HDFS = "mysql2hdfs";
    public static String MIGRATION_TYPE_HDFS2MYSQL = "hdfs2mysql";
    
    static {
        Path path = null;
        try {
            path = Paths.get(Main.class.getClassLoader().getResource("configuration.properties").toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        Properties configuration = new Properties();
        try {
            configuration.load(Files.newBufferedReader(path));
            MYSQL_HOST = configuration.getProperty("mysql.host", "192.168.100.100");
            MYSQL_PORT = configuration.getProperty("mysql.port", "3306");
            MYSQL_USER = configuration.getProperty("mysql.username", "root");
            MYSQL_PASSWORD = configuration.getProperty("mysql.password", "200081");
            MYSQL_DATABASE_MYSQL2HDFS = configuration.getProperty("mysql.database.mysql2hdfs", "gmall");
            MYSQL_DATABASE_HDFS2MYSQL = configuration.getProperty("mysql.database.hdfs2mysql", "gmall");
            MYSQL_URL_IMPORT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE_MYSQL2HDFS + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
            MYSQL_URL_EXPORT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE_HDFS2MYSQL + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
            MYSQL_TABLES_MYSQL2HDFS = configuration.getProperty("mysql.tables.mysql2hdfs", "");
            MYSQL_TABLES_HDFS2MYSQL = configuration.getProperty("mysql.tables.hdfs2mysql", "");
            IS_SEPERATED_TABLES = configuration.getProperty("is.seperated.tables", "0");
            HDFS_URI = configuration.getProperty("hdfs.uri", "hdfs://192.168.100.100:8020");
            OUT_DIR_MYSQL2HDFS = configuration.getProperty("out_dir_mysql2hdfs");
            OUT_DIR_HDFS2MYSQL = configuration.getProperty("out_dir_hdfs2mysql");
        } catch (IOException e) {
            MYSQL_HOST = "192.168.100.100";
            MYSQL_PORT = "3306";
            MYSQL_USER = "root";
            MYSQL_PASSWORD = "200081";
            MYSQL_DATABASE_MYSQL2HDFS = "gmall";
            MYSQL_DATABASE_HDFS2MYSQL = "gmall";
            MYSQL_URL_IMPORT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE_MYSQL2HDFS + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
            MYSQL_URL_EXPORT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE_HDFS2MYSQL + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
            MYSQL_TABLES_MYSQL2HDFS = "";
            MYSQL_TABLES_HDFS2MYSQL = "";
            IS_SEPERATED_TABLES = "0";
            HDFS_URI = "hdfs://192.168.100.100:8020";
            OUT_DIR_MYSQL2HDFS = null;
            OUT_DIR_HDFS2MYSQL = null;
        }
    }
    
    public static void main(String[] args) {
        System.out.println(MYSQL_DATABASE_HDFS2MYSQL);
    }
}
