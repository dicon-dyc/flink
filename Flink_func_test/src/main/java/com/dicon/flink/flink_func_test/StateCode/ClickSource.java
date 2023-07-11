package com.dicon.flink.flink_func_test.StateCode;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.RandomUtil;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.sql.Timestamp;

/**
 * @Author: dyc
 * @Create: 2023/6/27 14:22
 */

public class ClickSource implements SourceFunction<Event> {

    boolean isRunning = true;

    @Override
    public void run(SourceContext ctx) throws Exception {

        while (isRunning){

            int i = RandomUtil.randomInt(1,10);
            String userAccount = "JK0" + i;

            int j = RandomUtil.randomInt(1,10);
            String clickUrl = "http://dicon/" + j;

            Timestamp loginTime = DateUtil.date().toTimestamp();
            Thread.sleep(i*100);
            ctx.collect(new Event(userAccount,clickUrl,loginTime));

        }

    }

    @Override
    public void cancel() {

        isRunning = false;
    }
}
