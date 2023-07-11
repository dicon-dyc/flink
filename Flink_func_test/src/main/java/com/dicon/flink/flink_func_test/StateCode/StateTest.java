package com.dicon.flink.flink_func_test.StateCode;

import cn.hutool.core.date.DateUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;

/**
 * @Author: dyc
 * @Create: 2023/6/27 13:43
 */

public class StateTest {

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

        eventSingleOutputStreamOperator.keyBy(event -> event.getUserName())
                .flatMap(new MyKeyStateFlatMap());


        env.execute("statetest");

//        OutputTag<Event> eventsStream = new OutputTag<Event>("eventsStream"){};
//
//        SingleOutputStreamOperator<Event> process = eventSingleOutputStreamOperator.process(new ProcessFunction<Event, Event>() {
//            @Override
//            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
//                ctx.output(eventsStream, value);
//                System.out.println(value);
//                out.collect(value);
//            }
//        });
//
////        process.getSideOutput(eventsStream).print();
//
//        process.flatMap(new FlatMapFunction<Event, Tuple2<String,Integer>>() {
//            @Override
//            public void flatMap(Event o, Collector<Tuple2<String,Integer>> collector) throws Exception {
//
//               collector.collect(new Tuple2<>(o.getUserName(),1));
//            }
//        }).keyBy(x -> x.f0)
//          .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//          .sum(1)
//          .print()
//          .setParallelism(1);




    }

    public static class MyKeyStateFlatMap extends RichFlatMapFunction<Event,String> {

        ValueState<Event> myValueState;
        ListState<Event> myListState;
        MapState<String,Long> myMapState;
        ReducingState<Event> myReducingState;
        AggregatingState<Event,String> myAggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            myValueState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("myValueState",Event.class));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("myListState",Event.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String,Long>("myMapState",String.class,Long.class));

            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("myReducingState",
                    new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event event, Event t1) throws Exception {
                            return new Event(event.getUserName(),event.getClickUrl(),t1.getLoginTime());
                        }
                    }, Event.class));
            myAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event,Long,String>("myAggregatingState", new AggregateFunction<Event, Long, String>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(Event event, Long aLong) {
                    return aLong+1;
                }

                @Override
                public String getResult(Long aLong) {
                    return "count: "+aLong;
                }

                @Override
                public Long merge(Long aLong, Long acc1) {
                    return acc1+aLong;
                }
            }, Long.class));


        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
//            System.out.println(myValueState.value());
//            myValueState.update(event);
//            System.out.println("state value: " + myValueState.value());

            myListState.add(event);

            myMapState.put(event.userName,myMapState.get(event.getUserName()) == null ? 1 : myMapState.get(event.getUserName())+1);
            System.out.println("mapstate：" + event.userName + "value: " + " " + myMapState.get(event.getUserName()));

            myAggregatingState.add(event);
            System.out.println("aggregatingstate: " + myAggregatingState.get());

            myReducingState.add(event);
            System.out.println("reducingstate: " + myReducingState.get());

        }
    }
}
