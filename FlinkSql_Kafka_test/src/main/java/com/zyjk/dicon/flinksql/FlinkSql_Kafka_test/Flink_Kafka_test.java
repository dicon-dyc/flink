package com.zyjk.dicon.flinksql.FlinkSql_Kafka_test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink_Kafka_test {

    public static void main(String[] args) {

        //环境
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(bsEnv, bsSettings);

        //flinksql
        String Table_Create = "CREATE TABLE Flink_test_table()";

        //https://blog.csdn.net/qq_31963719/article/details/120058761
    }
}
