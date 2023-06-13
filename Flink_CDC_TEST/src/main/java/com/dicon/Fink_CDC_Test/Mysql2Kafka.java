package com.dicon.Fink_CDC_Test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Mysql -> FlinkCDC ->Kafka
 *
 * @author dyc
 * @date 2023-06-13
 *
 */
public class Mysql2Kafka {

    public static void main(String[] args) throws Exception {

        //TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO addsource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.70.48")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("usercontrol")
                .tableList("log_info")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"Mysql Source")
                .print();

        env.execute("Mysql2Kafka");

    }
}
