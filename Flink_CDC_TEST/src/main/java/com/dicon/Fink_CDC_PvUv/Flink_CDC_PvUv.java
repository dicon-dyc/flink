package com.dicon.Fink_CDC_PvUv;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author dyc
 * @date 2023/06/16
 */
public class Flink_CDC_PvUv {

    public static void main(String[] args) throws Exception {

        //TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 创建table执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        //TODO source
        TableResult inputTable = tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `log_info`(\n" +
                "    `id` bigint,\n" +
                "    `user_account` String,\n" +
                "    `login_time` TIMESTAMP(3),\n" +
                "    `click_url` String,\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = '192.168.70.48',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'usercontrol',\n" +
                "    'table-name' = 'log_info',\n" +
                "    'scan.incremental.snapshot.enabled' = 'false',\n" +
                "    'scan.startup.mode' = 'latest-offset')");

        //TODO create ods.log_info
        TableResult ODSTable = tableEnv.executeSql("CREATE TABLE IF NOT EXISTS ods_log_info(\n" +
                "    id bigint,\n" +
                "    user_account String,\n" +
                "    login_time TIMESTAMP(3),\n" +
                "    click_url String,\n" +
                "    WATERMARK FOR login_time AS login_time - INTERVAL '5' second\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'ods.log_info',\n" +
                "    'properties.bootstrap.servers' = 'master:9092,slave1:9092,slave2:9092',\n" +
                "    'sink.partitioner' = 'round-robin',\n" +
                "    'format'='debezium-json',\n" +
                "    'properties.group.id' = 'dwd_user_pvuv_test',\n" +
                "    'scan.startup.mode' = 'earliest-offset')");

        //TODO create dwd.user_pvuv
        TableResult DWSTable = tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dwd_user_pvuv(\n" +
                "    window_start TIMESTAMP(3),\n" +
                "    window_end TIMESTAMP(3),\n" +
                "    pv bigint,\n" +
                "    uv bigint\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'dwd.user_pvuv',\n" +
                "    'properties.bootstrap.servers' = 'master:9092,slave1:9092,slave2:9092',\n" +
                "    'format'='debezium-json',\n" +
                "    'properties.group.id' = 'dwd_user_pvuv_test',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'sink.partitioner'='round-robin')");

        //TODO create ads.report_user_pvuv
        TableResult ADSTable = tableEnv.executeSql("CREATE TABLE IF NOT EXISTS ads_report_user_pvuv(\n" +
                "    window_start TIMESTAMP(3),\n" +
                "    window_end TIMESTAMP(3),\n" +
                "    pv bigint,\n" +
                "    uv bigint,\n" +
                "    PRIMARY KEY(`window_start`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://192.168.56.104:3306/ads',\n" +
                "    'table-name' = 'report_user_pvuv',\n" +
                "    'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456' )");

        //Deprecated tansform
//        String sql = "SELECT count(user_account) AS pv\n" +
//                "FROM log_info li ";
//        String sql = "SELECT * FROM log_info";
//        Table table = tableEnv.sqlQuery(sql);
//        tableEnv.toChangelogStream(table).print();
//
//        env.execute("Flink_CDC_PvUv");

        //TODO mysql -> kafka ods
        tableEnv.executeSql("INSERT INTO ods_log_info\n" +
                "SELECT id\n" +
                "        ,user_account\n" +
                "        ,login_time\n" +
                "        ,click_url\n" +
                "FROM log_info");

        //TODO kafka ods -> kafka dwd
        tableEnv.executeSql("INSERT INTO dwd_user_pvuv\n" +
                "SELECT TUMBLE_START(ods_log_info.login_time,INTERVAL '1' minute ) AS window_start\n" +
                "     ,TUMBLE_END(ods_log_info.login_time,INTERVAL '1' minute ) AS window_end\n" +
                "     ,count(click_url) AS pv\n" +
                "     ,count(distinct user_account) AS uv\n" +
                "FROM ods_log_info\n" +
                "GROUP BY TUMBLE(ods_log_info.login_time,INTERVAL '1' minute )");

        //TODO kafka dwd -> mysql ads
        tableEnv.executeSql("INSERT INTO ads_report_user_pvuv\n" +
                "SELECT window_start\n" +
                "        ,window_end\n" +
                "        ,pv\n" +
                "        ,uv\n" +
                "FROM dwd_user_pvuv");


    }
}
