package com.cmcc.dyc.flink.MyStreamingWordCount;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyStreamingWordCount {

    public static void main(String[] args) throws Exception {

        //配置
        Properties properties = new Properties();
        //properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.171.128:9092");

        properties.setProperty("bootstrap.servers","192.168.171.128:9092");

        properties.setProperty("auto.offset.reset","latest");




        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从kafka读取数据
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer<String>("f-k-s",new SimpleStringSchema(),properties));

        //打印数据
        kafkaDS.print();

        //执行任务
        env.execute();



    }
}
