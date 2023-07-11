package com.dicon.flink.flink_func_test.PvUv;

import cn.hutool.core.date.DateUtil;
import com.dicon.flink.flink_func_test.StateCode.ClickSource;
import com.dicon.flink.flink_func_test.StateCode.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Int;


import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @Author: dyc
 * @Create: 2023/6/28 13:35
 */
public class AggregatePuUv {

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

        eventSingleOutputStreamOperator.print();

        eventSingleOutputStreamOperator.keyBy(event -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .aggregate(new MyAggregateFunctionAgg(),new MyProcessWindowFuntionAgg())
                .print();

        env.execute();


    }

    /**
     * 自自定processwindowfunction获取窗口启停时间
     * @Author: dyc
     * @Create: 2023/7/4 23:38
     */
    public static class MyProcessWindowFuntionAgg extends ProcessWindowFunction<Tuple2<Integer,Integer>, String, Boolean, TimeWindow>{

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            Timestamp startTime = DateUtil.date(start).toTimestamp();
            long end = context.window().getEnd();
            Timestamp endTime = DateUtil.date(end).toTimestamp();
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("pv: " + elements.iterator().next().f0 + "\n");
            stringBuffer.append("uv: " + elements.iterator().next().f1 + "\n");
            stringBuffer.append("startTime: " + startTime + "\n");
            stringBuffer.append("endTime: " + endTime + "\n");
            out.collect(stringBuffer.toString());
        }
    }

    /**
     * 自定义aggregatefunction获取pvuv
     * @Author: dyc
     * @Create: 2023/7/4 23:39
     */
    public static class MyAggregateFunctionAgg implements AggregateFunction<Event, Tuple2<Integer, HashSet<String>>, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, HashSet<String>> createAccumulator() {
            return Tuple2.of(0, new HashSet<>());
        }

        @Override
        public Tuple2<Integer, HashSet<String>> add(Event event, Tuple2<Integer, HashSet<String>> integerHashSetTuple2) {
            integerHashSetTuple2.f1.add(event.userName);
            return Tuple2.of(integerHashSetTuple2.f0 + 1, integerHashSetTuple2.f1);
        }

        @Override
        public Tuple2<Integer, Integer> getResult(Tuple2<Integer, HashSet<String>> integerHashSetTuple2) {
            return Tuple2.of(integerHashSetTuple2.f0, integerHashSetTuple2.f1.size());
        }

        @Override
        public Tuple2<Integer, HashSet<String>> merge(Tuple2<Integer, HashSet<String>> integerHashSetTuple2, Tuple2<Integer, HashSet<String>> acc1) {
            return null;
        }
    }
}
