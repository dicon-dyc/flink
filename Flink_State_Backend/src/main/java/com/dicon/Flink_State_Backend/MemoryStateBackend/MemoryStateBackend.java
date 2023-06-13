package com.dicon.Flink_State_Backend.MemoryStateBackend;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 * @author 13511
 * @date 2023/4/19
 */

public class MemoryStateBackend {

    public static void main(String[] args) throws Exception {

        //TODO 创建执行环境并配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //并行度为3
        env.setParallelism(3);

        //配置checkpoint时间间隔以及配置Mode为exactly_once
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        //配置超时时间，默认10分钟
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(5*1000);

        //配置statebackend
        env.setStateBackend(new org.apache.flink.runtime.state.memory.MemoryStateBackend());

        //TODO 配置source并读取
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.56.104",9999);

        //TODO 计算
        DataStream<Tuple2<String,Integer>> resultDataStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (int i = 0; i < words.length; i++) {
                    collector.collect(new Tuple2<>(words[i],1));
                }
            }
        }).keyBy(x -> x.f0)
                .sum(1);

        //TODO 输出
        resultDataStream.addSink(JdbcSink.sink(
                "Insert INTO MemoryWordCount (words,counts) VALUES(?,?)",
                (preparedStatement, stringIntegerTuple2) -> {
                    preparedStatement.setString(1,stringIntegerTuple2.f0);
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(200)
                        .withBatchSize(1000)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.16.218:3306/flink")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));


//        resultDataStream.print();

        env.execute();


    }

}

//    // 创建环境
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//// 指定了checkpoint的时间间隔以及配置Mode为保持State的一致性
//env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
//// 也可以这么配置
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//// 配置Checkpoint彼此之间的停顿时间（即限制在某段时间内，只能有一个Checkpoint进行）单位毫秒
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000);
//// 配置Checkpoint的并发量（比如某些程序的Checkpoint生产需要很长时间，可以通过这种方式加大效率）
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);
//// 配置Checkpoint的超时时间（避免Checkpoint生产时间过长）默认10分钟
//        env.getCheckpointConfig().setCheckpointTimeout(5 * 1000);
//// 配置Checkpoint失败的最大容忍次数，默认0次，如果超过了这个次数，则认为Checkpoint失败
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
//// 配置Checkpoint的时间间隔，单位毫秒
//        env.getCheckpointConfig().setCheckpointInterval(1000);
//// 配置Checkpoint的存放的文件路径
//        env.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints"));
//// Checkpoint默认的配置是失败了，就重启恢复。因此当一个Flink失败/人为取消的时候，Checkpoint会被人为清除
//// 配置Checkpoint开启 外化功能 。即应用程序停止时候，保存Checkpoint
//// 支持2种外化：DELETE_ON_CANCELLATION：当应用程序完全失败或者明确地取消时，保存 Checkpoint。
////              RETAIN_ON_CANCELLATION：当应用程序完全失败时，保存 Checkpoint。如果应用程序是明确地取消时，Checkpoint 被删除。
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
