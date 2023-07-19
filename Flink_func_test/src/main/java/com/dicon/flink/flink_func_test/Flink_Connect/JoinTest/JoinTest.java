package com.dicon.flink.flink_func_test.Flink_Connect.JoinTest;

import com.dicon.flink.flink_func_test.StateCode.ClickSource;
import com.dicon.flink.flink_func_test.StateCode.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Author: dyc
 * @Create: 2023/7/18 15:51
 */

public class JoinTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getLoginTime().getTime();
                            }
                        }));

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator1 = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getLoginTime().getTime();
                            }
                        }));

        DataStreamSink<String> print = eventSingleOutputStreamOperator.join(eventSingleOutputStreamOperator1)
                .where(x -> x.userName)
                .equalTo(x -> x.userName)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Event, Event, String>() {

                    @Override
                    public String join(Event event, Event event2) throws Exception {
                        return event.toString() + "-------" + event2;
                    }
                })
                .print();

        env.execute();
    }




}
