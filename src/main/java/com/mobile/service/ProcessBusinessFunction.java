package com.mobile.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobile.entity.Alarm;
import com.mobile.entity.AlarmResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

@Slf4j
public class ProcessBusinessFunction extends ProcessWindowFunction<Alarm, String, String, TimeWindow> {


    @Override
    public void process(String key, ProcessWindowFunction<Alarm, String, String, TimeWindow>.Context context, Iterable<Alarm> iterable, Collector<String> collector) throws Exception {
        try {
            // 打印窗口时间跟水印时间 log.info("Element: time={}, watermark={}", context.currentProcessingTime(), context.currentWatermark());
            AlarmResult alarmReuslt = new AlarmResult();
            ArrayList<JSONObject> list = new ArrayList<>();
            iterable.forEach(item -> {
                JSONObject json = new JSONObject();
                json.put("CREATE_TIME",item.getCreateTime());
                json.put("PAYLOAD",item.getPlayload());
                json.put("REQUEST_PAYLOAD",item.getRequestPlayload());
                json.put("RESPONSE_MESSAGE",item.getResponseMsg());
                list.add(json);
            });
            alarmReuslt.setRecong(key);
            alarmReuslt.setBusinessList(String.valueOf(list));
            // collector.collect(JSON.toJSONString(alarmReuslt));
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(alarmReuslt);
            collector.collect(json);
            log.info("当前聚合数据key:{}",key);
        } catch (Exception e) {
            log.error("process business error ", e);
        }
    }
}
