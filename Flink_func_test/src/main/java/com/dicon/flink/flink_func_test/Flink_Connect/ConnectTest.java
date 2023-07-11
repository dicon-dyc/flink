package com.dicon.flink.flink_func_test.Flink_Connect;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author: dyc
 * @Create: 2023/7/5 12:45
 */
public class ConnectTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> dataSource1 = env.fromElements(1, 2, 3);

        DataStreamSource<Long> dataSource2 = env.fromElements(1L, 2L, 3L);

        dataSource1.connect(dataSource2)
                .map(new CoMapFunction<Integer, Long, Long>() {
                    @Override
                    public Long map1(Integer value) throws Exception {
                        return Long.parseLong(String.valueOf(value));
                    }

                    @Override
                    public Long map2(Long value) throws Exception {
                        return value;
                    }
                })
                .print();

        env.execute();
    }
}
