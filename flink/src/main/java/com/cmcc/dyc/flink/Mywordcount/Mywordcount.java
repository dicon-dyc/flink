package com.cmcc.dyc.flink.Mywordcount;

/***
 * author:dicon
 * date:2022-08-09
 * func:flinkwordcounttest
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Mywordcount {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost",10240,"\n");

        //parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        for (String word : value.split("\\s")){
                            out.collect(new WordWithCount(word,1L));
                        }
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5),Time.seconds(1))
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        return new WordWithCount(a.word,a.count+b.count);
                    }
                });

        //print the results with a single thread,rather than in parallel
        windowCounts.print().setParallelism(1);
        env.execute("My Window WordCount");

    }

    //Data type for words with count
    public static class WordWithCount{

        public String word;
        public long count;

        public WordWithCount(){

        }

        public WordWithCount(String word,long count){
            this.word = word;
            this.count = count;
        }

        public String toString(){
            return word + ":" + count;
        }
    }
}