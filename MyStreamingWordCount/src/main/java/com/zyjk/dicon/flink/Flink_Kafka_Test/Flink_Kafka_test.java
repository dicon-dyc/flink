package com.zyjk.dicon.flink.Flink_Kafka_Test;


/**
 * author:diaoyachong
 * date:2022-08-16
 * func:测试flink消费kafka数据
 */


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class Flink_Kafka_test {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //参数设置
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        //配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.171.128:9092");
        properties.setProperty("group.id","group_test");

        /**
         * 打开动态分区发现功能
         * 每隔10ms会动态获取Topic的元数据，对于新增的Partition会自动从最早的位点开始消费数据。
         * 防止新增的分区没有被及时发现导致数据丢失，消费者必须要感知Partition的动态变化
         */

        properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,"10");

        //动态地发现Topic，可以指定Topic的正则表达式
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                "f-k-s",
                new SimpleStringSchema(),
                properties
        );


        //消费多个Topic
        /**
         *         List<String> topics = new LinkedList<>();
         *         topics.add("f-k-s");
         *         FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
         *                 topics,
         *                 new SimpleStringSchema(),
         *                 properties
         *         );
         */

        //设置从最早的offset消费
        consumer.setStartFromEarliest();

        env.addSource(consumer).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                System.out.println(s);
            }
        });

        env.execute();
    }
}
