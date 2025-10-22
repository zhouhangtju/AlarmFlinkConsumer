package com.mobile.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.util.List;

@Data
public class Alarm {
    @JSONField(name = "SRC_IP")
    private String srcIp;
    @JSONField(name = "DST_IP")
    private String dstIp;
    @JSONField(name = "EVENT_NAME")
    private String eventName;

    @JSONField(name = "PAYLOAD")
    private String playload;
    @JSONField(name = "REQUEST_PAYLOAD")
    private String requestPlayload;
    @JSONField(name = "RESPONSE_MESSAGE")
    private String responseMsg;
    @JSONField(name = "CREATE_TIME")
    private String createTime;
    private Long time;
    @JSONField(name = "DEVICE_TYPE")
    private String deviceType;

    private String recong;

    private List<String> businessList;

}
