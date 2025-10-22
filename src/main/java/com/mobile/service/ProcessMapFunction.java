package com.mobile.service;

import com.alibaba.fastjson2.JSON;
import com.mobile.entity.Alarm;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

@Slf4j
public class ProcessMapFunction implements MapFunction<String, Alarm> {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.of("Asia/Shanghai"));

    private static final ZoneId SHANGHAI_ZONE = ZoneId.of("Asia/Shanghai");

    @Override
    public Alarm map(String s) throws Exception {
        try {
            Alarm alarm = JSON.parseObject(s,Alarm.class);
            /*SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));*/
            if (StringUtils.isNotEmpty(alarm.getCreateTime())) {
                LocalDateTime time = LocalDateTime.parse(alarm.getCreateTime(), FORMATTER);
                alarm.setTime(time.atZone(SHANGHAI_ZONE).toInstant().toEpochMilli());
               // alarm.setTime(System.currentTimeMillis());
            } else {
                alarm.setTime(System.currentTimeMillis());
            }
            if (StringUtils.isNotEmpty(alarm.getSrcIp()) && StringUtils.isNotEmpty(alarm.getDstIp())) {
                alarm.setRecong(alarm.getSrcIp() + "_" +alarm.getDstIp()+"_"+alarm.getEventName());
            }
            return alarm;

        } catch (Exception e) {
            log.error("", e);
        }
        // 不符合条件的返回新创建的
        return new Alarm();
    }

}
