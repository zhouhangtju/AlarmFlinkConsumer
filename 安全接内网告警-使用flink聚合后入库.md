# 安全接内网告警-flink聚合数据

## 1.功能介绍

使用flink聚合上游kafka内网告警数据，将聚合后的数据发到98服务器的kafka上，再将聚合后的数据入库到postgresql中。

## 2.主要接口和项目结构

### 2.1flink聚合程序项目结构说明

```
src
└── main
    ├── java
    │   └── com.mobile
    │       ├── entity          # 实体类包，用于映射数据模型
    │       │   ├── Alarm       # 告警信息实体类
    │       │   └── AlarmResult # 告警结果实体类
    │       ├── service         # 业务功能包，包含数据处理相关的业务逻辑
    │       │   ├── ProcessBusinessFunction # 业务处理功能类（核心业务逻辑实现）
    │       │   ├── ProcessFilterFunction   # 数据过滤功能类（对数据进行筛选）
    │       │   ├── ProcessMapFunction      # 数据映射功能类（数据格式转换/字段映射）
    │       │   └── ProcessSinkFunction     # 数据输出功能类（将处理后的数据写入目标存储）
    │       └── Application     # 项目启动类（负责初始化并启动应用）
    └── resources               # 配置文件目录
        ├── flink.properties    # Flink 相关配置文件
        └── logback.xml         # 日志框架配置文件
```

核心代码 位于Application下，这段代码从 Kafka 读取数据，经过过滤、映射和时间戳分配，按 `recong` 字段分组，在 10 分钟滚动窗口内进行业务处理，最后将结果写入 Kafka。

```
   final StreamExecutionEnvironment streamEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            streamEnvironment.setParallelism(6);
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
```



### 2.2聚合数据入库项目结构说明

```
src
└── main
    ├── java
    │   └── com.mobile.outalarm2
    │       ├── common                 # 公共工具类和枚举类
    │       │   ├── CommonResult       # 统一返回结果封装类，标准化接口返回数据
    │       │   ├── CommonResultEnum   # 返回结果枚举类型，定义成功、失败等状态码
    │       │   ├── CsvUtil            # CSV 文件读写工具类
    │       │   └── ExecutorConfig     # 线程池配置类，统一管理异步任务线程池
    │       ├── controller             # 控制层，接收前端请求并返回响应
    │       │   └── DemoController     # 示例控制器，用于测试接口或演示功能
    │       ├── dao                    # 数据访问层（Mapper 接口）
    │       │   ├── AlarmResultAlldayDao # 告警结果（全天）数据访问接口
    │       │   └── AlarmResultDao     # 告警结果数据访问接口
    │       ├── db                     # 数据库入库实体类
    │       ├── kafka                  # Kafka 消息队列相关代码
    │       │   ├── KafkaConsumer      # Kafka 消息消费者类 主要业务逻辑
    │       │   └── KafkaProducer.java # Kafka 消息生产者类(暂未使用)
    │       ├── service                # 业务逻辑层
    │       │   ├── impl
    │       │   │   └── AlarmResultServiceImpl # 告警结果业务接口的实现类，包含具体业务逻辑
    │       │   └── AlarmResultService # 告警结果业务接口
    │       ├── task                   # 定时任务相关代码
    │       │   └── TaskService        # 定时任务业务类，用于定时执行数据统计、告警检测等任务
    │       ├── util                   # 工具类目录
    │       │   └── DbUtil             # 数据库操作工具类，封装数据库连接、关闭等方法
    │       └── OutAlarm2Application   # 项目启动类，包含 main 方法，启动 Spring Boot 应用
    └── resources                      # 配置文件和资源文件目录
        ├── mapper                     # MyBatis 的 Mapper XML 文件目录，存放 SQL 映射文件
        ├── application.yml            # Spring Boot 主配置文件，包含全局配置（端口、数据库连接等）
        ├── application-dev.yml        # 开发环境配置文件
        ├── application-prod.yml       # 生产环境配置文件
        ├── application-test.yml       # 测试环境配置文件
        └── logback.xml                # Logback 日志框架配置文件，定义日志输出格式、路径、级别等
```

核心代码，位于KafkaConsumer下

负责接收kafka中聚合后的告警数据，整理后入库到postgresql中。

```
    public void topic_alarm(List<String> messages, Acknowledgment ack) {
        log.info("获取到kakfa消息条数{},线程{}", messages.size(), Thread.currentThread().getName());
        Optional message = Optional.ofNullable(messages);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); // 设置为东八区
            try {
                if (message.isPresent()) {
                    HashSet<String> dsSet = new HashSet<>();
                    String tableDs = new String();
                    ArrayList<AlarmResult> alarmResults = new ArrayList<>();
                    for (int i = 0; i < messages.size(); i++) {
                        String s = messages.get(i);
                    JSONObject jsonObject = JSON.parseObject(s);
                    JSONArray businessJsonList = jsonObject.getJSONArray("businessList");
                    List<Business> businessList = businessJsonList.toJavaList(Business.class);
                    String recong = jsonObject.getString("recong");
                    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
                    AlarmResult alarmResult = new AlarmResult();
                    String[] split = recong.split("_");
                    //判断同一个批数据会不会跨天
                    //按小时聚合，一天一张表
                    Map<String, List<Business>> collect = businessList.stream().collect(Collectors.groupingBy(Business::getCREATE_TIME));
                    ArrayList<String> timeList = new ArrayList<>();
                    collect.keySet().forEach(key -> {
                        String dateHourPart = key.substring(0, 13);
                        // 去除所有非数字字符，得到 "2025082516"
                        String ds = dateHourPart.replaceAll("[^0-9]", "");
                        dsSet.add(ds);
                        timeList.add(key);
                    });
//                    //dsSet数量大于1，即为跨天
                    List<String> dsList = dsSet.stream().collect(Collectors.toList());
                        tableDs = dsList.get(0).substring(0,8);
                        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        sdf2.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); // 设置为东八区
                        alarmResult.setLastTime(sdf2.parse(timeList.get(0)));
                        alarmResult.setDs(dsList.get(0));
                        alarmResult.setEventName(split[2]);
                        alarmResult.setSrcIp(split[0]);
                        alarmResult.setDstIp(split[1]);
                        alarmResult.setResult(JSON.toJSONString(businessJsonList));
                        alarmResult.setKey(recong);
                        alarmResults.add(alarmResult);
//                    }
                  }
                    if(alarmResults.size()!=0){
                        log.info("收到的数据{},{}", alarmResults.get(0).getKey(),alarmResults.get(0).getLastTime());
                    }
                    alarmResultDao.insertBatch(alarmResults,"alarm_result_"+tableDs);
                }
            } catch (Exception e) {
                log.info("消费数据故障:{}", e);
            } finally {
                ack.acknowledge();
            }
        }
```

## 3.数据库信息

```
建表语句

 1.聚合数据表
CREATE TABLE public.alarm_result_20251030 (
	"key" varchar NULL,
	"result" varchar NULL,
	ds varchar NULL,
	src_ip varchar NULL,
	dst_ip varchar NULL,
	event_name varchar NULL,
	last_time timestamp NULL
);


```

## 4.服务器配置信息

```
188.107.240.98
部署从kafka接收聚合后数据的


1.pgsql数据映射目录
/data/outalarm/pgsqldata

docker run --name pgsql -p 5432:5432 -e POSTGRES_PASSWORD=QWER@1234 -v /data/outalarm/pgsqldata:/var/lib/postgresql/data --restart=always -d postgres:15.14-trixie

docker exec -it pgsql /bin/bash
psql -U postgres -d postgres
SET search_path TO public;
\dt


2.java全局环境 jdk8
/data/jdk

3.kafak
/data/kafka


188.107.240.99

部署flink聚合内网告警数据程序。
1.jdk17
/data/jdk


 

```

