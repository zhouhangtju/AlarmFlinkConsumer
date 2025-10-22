package com.mobile;

import com.alibaba.fastjson2.JSON;
import com.mobile.entity.Alarm;
import com.mobile.service.ProcessBusinessFunction;
import com.mobile.service.ProcessFilterFunction;
import com.mobile.service.ProcessMapFunction;
import com.mobile.service.ProcessSinkFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.io.*;
import java.time.Duration;
import java.util.Properties;
import java.lang.management.ManagementFactory;


@Slf4j
public class Application {

    public static void main(String[] args) {
        try {
            System.out.println("=== 当前 JVM 启动参数 ===");
            ManagementFactory.getRuntimeMXBean().getInputArguments()
                    .forEach(arg -> System.out.println("JVM Arg: " + arg));
            System.out.println("=====================");
            Properties property = getProperty();

            // flink UI的端口
            String flinkUiPort = property.getProperty("rest.port");
            String finalImportUrl = property.getProperty("bootstrap.servers");

            log.info("===start 开始 ===={}, {}", flinkUiPort, finalImportUrl);

            // 配置消费者属性
            Properties consumerProperties = new Properties();
            consumerProperties.setProperty("bootstrap.servers", "10.209.44.87:9092,10.209.44.103:9092,10.209.44.114:9092");
            //consumerProperties.setProperty("bootstrap.servers", "172.18.17.113:9092");
            consumerProperties.setProperty("group.id", "test");
            consumerProperties.put("key.deserializer", property.getProperty("key.deserializer"));
            consumerProperties.put("value.deserializer", property.getProperty("value.deserializer"));
            consumerProperties.put("auto.offset.reset", property.getProperty("auto.offset.reset"));

            // 配置sink属性
            Properties sinkProperties = new Properties();
            sinkProperties.setProperty("bootstrap.servers", "188.107.245.53:9092");
           // sinkProperties.setProperty("bootstrap.servers", "172.18.17.113:9092");
            sinkProperties.setProperty("group.id", "sink");
            sinkProperties.setProperty("transaction.timeout.ms", "60000");
            sinkProperties.setProperty("enable.idempotence", "true");
            sinkProperties.setProperty("max.request.size", "524288000");
            sinkProperties.setProperty("buffer.memory", "524288000");
            // 创建Kafka消费者
            FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                    "asap_superset", // 输入的Kafka主题
                   // "matrixTest", // 输入的Kafka主题
                    new SimpleStringSchema(),
                    consumerProperties);
            // 创建kafka的sink
            FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("matrix",
                    new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                    sinkProperties,
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // 确保消息传递语义

            // 数据时间
            SerializableTimestampAssigner<Alarm> timestampAssigner = new SerializableTimestampAssigner<Alarm>() {
                @Override
                public long extractTimestamp(Alarm element, long recordTimestamp) {
                    return element.getTime();
                }
            };

            Configuration configuration = new Configuration();
            configuration.setString("rest.port", String.valueOf(flinkUiPort));
// 总内存8GB
            configuration.setString("taskmanager.memory.process.size", "8192m");
// 网络缓冲区占比20%（计算值1638m）
            configuration.setString("taskmanager.memory.network.fraction", "0.2");

            final StreamExecutionEnvironment streamEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);





            streamEnvironment.setParallelism(1);
            // 数据延迟时间
            Integer delay = Integer.valueOf(60);
            // 添加数据源
            streamEnvironment.addSource(consumer)
                    .filter(new ProcessFilterFunction())
                    .map(new ProcessMapFunction())
                    .filter(alarm -> {
                        return alarm != null && !StringUtils.isEmpty(alarm.getRecong());
                    })
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<Alarm>forBoundedOutOfOrderness(Duration.ofSeconds(delay))
                            .withTimestampAssigner(timestampAssigner))
                    .keyBy(value -> value.getRecong())
                    .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                    .process(new ProcessBusinessFunction())
                    .addSink(kafkaProducer)
                    .setParallelism(Integer.valueOf(property.getProperty("sink.parallelism"))); // sink的并行度，默认是8;
            log.info("=====start process======");
            // 执行程序
            streamEnvironment.execute("Flink out alarm");

        } catch (Exception e) {
            log.info("===error: ", e);
        }
    }

    public static String getCurPath() {
        try {
            // 获取当前类的.class文件或者jar包的路径
            String jarPath = Application.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
            // 将路径转换为File对象，方便操作
            File jarFile = new File(jarPath);
            // 获取jar文件所在目录
            String jarDir = jarFile.getParentFile().getAbsolutePath();
            System.out.println("JAR所在目录: " + jarDir);
            return jarDir;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Properties getProperty() {
        String curPath = getCurPath();
        log.info("Current path is {}", curPath);
        String configPath = curPath + "/config/flink.properties";
        File configFile = new File(configPath);
        Properties property = new Properties();
        if (configFile.exists()) {
            try(FileReader reader = new FileReader(configFile)) {
                property.load(reader);
            } catch (Exception e) {
                log.error("load flink.properties error ", e);
            }
        } else {
            try (InputStream inputStream = Application.class.getClassLoader().getResourceAsStream("flink.properties")) {
                if (inputStream == null) {
                    throw new FileNotFoundException("flink.properties file not found in classpath");
                }
                property.load(inputStream);
            } catch (IOException e) {
                log.error("load config flink.properties error ", e);
            }
        }
        return property;

    }

}
