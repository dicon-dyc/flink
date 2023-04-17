package com.dicon.flink.flink_func_test.streamOperators;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.Int;

/**
 * Author:dyc
 * Date:2023/04/17
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class orderPojo {

    private Integer orderId;

    private Integer orderAmount;

}
