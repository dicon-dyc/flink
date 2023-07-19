package com.dicon.flink.flink_func_test.StateCode.MyOperationState;

import com.dicon.flink.flink_func_test.StateCode.ClickSource;
import com.dicon.flink.flink_func_test.StateCode.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sun.java2d.pipe.hw.AccelDeviceEventNotifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class MyOperationState {

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

//        eventSingleOutputStreamOperator.print();

        eventSingleOutputStreamOperator.addSink(new MyBufferSink(10));

        env.execute();


    }

    public static class MyBufferSink implements SinkFunction<Event>, CheckpointedFunction{

        //定义当前类的属性，批量
        private final int threshold;

        private List<Event> bufferedElements;

        //定义一个算子状态
        private ListState<Event> checkpointState;

        public MyBufferSink(int threshold) {
            this.threshold =  threshold;
            this.bufferedElements = new ArrayList<>();
        }



        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            checkpointState.clear();
            for (Event bufferedElement : bufferedElements) {
                checkpointState.add(bufferedElement);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            ListStateDescriptor<Event> bufferdstate = new ListStateDescriptor<>("bufferdstate", Event.class);

            checkpointState = context.getOperatorStateStore().getListState(bufferdstate);

            //故障恢复=
            if (context.isRestored()){
                for (Event event : checkpointState.get()) {
                    bufferedElements.add(event);
                }
            }
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);

            //判断是否超过阈值
            if (bufferedElements.size() == threshold){
                for (Event bufferedElement : bufferedElements) {
                    System.out.println(bufferedElement);
                }
                System.out.println("===============================");
                bufferedElements.clear();
            }


        }
    }
}
