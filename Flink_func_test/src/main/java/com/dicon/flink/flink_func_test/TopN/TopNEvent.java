package com.dicon.flink.flink_func_test.TopN;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * TopN 实体类
 * @Author: dyc
 * @Create: 2023/7/4 15:13
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopNEvent implements Serializable {

    public String userName;
    public String url;
    public Timestamp clickTime;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Timestamp getClickTime() {
        return clickTime;
    }

    public void setClickTime(Timestamp clickTime) {
        this.clickTime = clickTime;
    }
}
