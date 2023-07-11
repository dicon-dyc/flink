package com.dicon.flink.flink_func_test.TopN;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 *
 * 统计热门url，每5秒更新一次
 * @Author: dyc
 * @Create: 2023/7/4 15:11
 */

public class TopN {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<TopNEvent> dataSource = environment.addSource(new TopNSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TopNEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<TopNEvent>() {

                            @Override
                            public long extractTimestamp(TopNEvent topNEvent, long l) {
                                return topNEvent.getClickTime().getTime();
                            }
                        }));

//        dataSource
        environment.execute();

    }
}
