package com.mobile.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobile.entity.Alarm;
import com.mobile.entity.AlarmResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class ProcessSinkFunction extends RichSinkFunction<AlarmResult> {

    private transient FlinkKafkaProducer<String> kafkaProducer;
    private String topic;
    private Properties kafkaProperties;

    public ProcessSinkFunction(String topic, Properties kafkaProperties) {
        super();
        this.topic = topic;
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        // 初始化Kafka producer等操作
        kafkaProducer = new FlinkKafkaProducer<>(topic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), // 自定义序列化逻辑
                kafkaProperties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // 确保消息传递语义
    }

    @Override
    public void invoke(AlarmResult alarm,Context context) throws Exception {
        // 将Alarm对象转换为字符串
        String alarmAsString = alarm.toString();
        // 发送字符串到Kafka
        log.info("发送字符串到Kafka{}",alarm);
        try {
            kafkaProducer.invoke(alarmAsString);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }
}
