package com.mobile.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;

@Slf4j
public class ProcessFilterFunction implements FilterFunction<String> {

    @Override
    public boolean filter(String value) throws Exception {
       // log.info("收到一条消息{}",value);
        // 快速检查是否包含目标 device_type，避免完整解析
        return value != null
                &&
                value.contains("\"DEVICE_TYPE\":\"Nsfocus_FLOW_DCN\"");
                // 可以再加一点判断，比如是否包含 srcIp/dstIp 字段（如果字段名固定）
//                && value.contains("\"srcIp\"") &&
//                value.contains("\"dstIp\"");
    }
}
