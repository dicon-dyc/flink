package com.dicon.flink.flink_func_test.StateCode;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @Author: dyc
 * @Create: 2023/6/27 14:19
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Event implements Serializable {

    public String userName;
    public String clickUrl;
    public Timestamp loginTime;
}
