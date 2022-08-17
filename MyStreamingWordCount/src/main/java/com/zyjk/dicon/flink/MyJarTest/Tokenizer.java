package com.zyjk.dicon.flink.MyJarTest;


/**
 * author:diaoyachong
 * date:2022-08-16
 * func:lines->(word,1)
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public  class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>> {

    @Override
    public void flatMap(String in, Collector<Tuple2<String,Integer>> out) throws Exception {

        //获取每行数据，拆分为单个单词
        String[] words = in.toLowerCase().split("\\W+");

        System.out.println(in);

        //将每个单词转化为（word，1）的格式
        for (String word : words) {

            out.collect(new Tuple2<String,Integer>(word,1));
        }

    }

}