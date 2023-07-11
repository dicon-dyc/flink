package com.dicon.flink.flink_func_test.TopN;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.RandomUtil;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;

/**
 * TopNsrouce实现
 * @Author: dyc
 * @Create: 2023/7/4 15:16
 */

public class TopNSource implements SourceFunction<TopNEvent> {

    boolean isRunning = true;

    @Override
    public void run(SourceContext ctx) throws Exception {

        while (isRunning){

            int i = RandomUtil.randomInt(5);
            String userName = "jk0"+i;

            int j = RandomUtil.randomInt(5);
            String url = "https://dicon:"+j;

            Timestamp timestamp = DateUtil.date().toTimestamp();
            Thread.sleep(100L);
            ctx.collect(new TopNEvent(userName,url,timestamp));
        }

    }

    @Override
    public void cancel() {

        isRunning = false;
    }
}
