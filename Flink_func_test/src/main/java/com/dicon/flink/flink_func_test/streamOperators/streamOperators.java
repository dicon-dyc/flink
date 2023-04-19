package com.dicon.flink.flink_func_test.streamOperators;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author:dyc
 * Date:2023-04-16
 */
public class streamOperators {
    public static void main(String[] args) throws Exception {

        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 配置source
        /**
         * home:192.168.245.141
         * work:192.168.56.103
         */
        DataStreamSource streamSource = env.socketTextStream("192.168.56.103",9999);

        //TODO 计算

        /**
         * map:将dataStream中的每个元素转换为另一个元素
         */
//        DataStream<String> resultData = streamSource.map(x -> "map : " + x);

        /**
         * flatMap:获取一个数据元并生成0，1，n个数据元。
         */
//        DataStream<String> resultData = streamSource.flatMap(new FlatMapFunction<String,String>() {
//
//            @Override
//            public void flatMap(String input, Collector collector) throws Exception {
//
//                String[] words = input.split(" ");
//                for (int i = 0; i < words.length; i++) {
//                    collector.collect(words[i]);
//                }
//            }
//        });

        /**
         * filter:计算每个数据元的布尔函数，并保存函数返回true的数据元。过滤掉零值的过滤器。
         */
//        DataStream<String> resultData = streamSource.filter(x -> x.equals("hello"));

        /**
         * KeyBy:逻辑上将流分区为不相交的分区，具有相同Keys的所有记录都分配给同一分区。
         *
         * 保证key相同的一定进入到一个分区内，但是一个分区内可以有多key的数据；
         * 是对数据进行实时的分区，不是上游发送给下游，而是将数据写入到对应的channel的缓存中，下游到上游实时拉取；
         * keyBy底层是new KeyedStream，然后将父DataStream包起来，并且传入keyBy的条件（keySelector）；
         * 最终会调用KeyGroupStreamPartitioner的selectChannel方法，将keyBy的条件的返回值传入到该方法中；
         * 流程：
         * 1、先计算key的HashCode值（有可能会是负的）
         * 2、将key的HashCode值进行特殊的hash处理，MathUtils。murmurHash（keyHash），一定返回正数，避免返回的数字为负
         * 3、将返回特殊的hash值模除以最大并行度，默认是128，得到keyGroupId
         * 4、keyGroupId*parallelism（程序的并行度）/maxParallism（默认最大并行），返回分区编号
         * 注意：1.如果将自定义pojo当成key，必须重写hashcode方法。2.不能将数组当成keyby的key。
         *
         */

        //DataStream<String> resultData = streamSource.keyBy().

        /**
         * Reduce:被Keys化数据流上的“滚动”Reduce。将当前数据元与最后一个Reduce的值组合并发出新值
         */



        //TODO 输入
        //resultData.print();
        env.execute();


    }
}
