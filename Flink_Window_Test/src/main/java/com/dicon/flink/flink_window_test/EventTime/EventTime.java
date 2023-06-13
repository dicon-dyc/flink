package com.dicon.flink.flink_window_test.EventTime;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * {"time":"","word":""}
 *
 * {"time":"2023-06-07 14:35:00","word":"hello"}
 * {"time":"2023-06-07 14:36:00","word":"hello"}
 * {"time":"2023-06-07 14:45:06","word":"hello"}
 * {"time":"2023-06-07 14:47:07","word":"hello"}
 * {"time":"2023-06-07 14:47:08","word":"hello"}
 *
 */
public class EventTime {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("master", 9999);

        long maxOutOfOrderness = 2*1000L;

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(maxOutOfOrderness))
                        .withTimestampAssigner((element, recodeTimestamp) -> {
                            JSONObject jsonObject = JSONObject.parseObject(element);
                            SimpleDateFormat parse = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                            Date date = null;
                            try {
                                date = parse.parse(jsonObject.getString("time"));
                            } catch (ParseException e) {
                                throw new RuntimeException(e);
                            }
                            return date.getTime();
                        }))
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

                        JSONObject jsonObject = JSONObject.parseObject(s);
                        collector.collect(new Tuple2<>(jsonObject.getString("word"),1));

                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1);

        sum.print();

        env.execute("eventtime");

    }
}
