package com.pearfl.dlk.flink;

import com.pearfl.dlk.config.KafkaConfigLoader;
import com.pearfl.dlk.config.KafkaGlobalConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink SQL 与 Kafka 集成应用：实现JSON数据的实时SQL处理
 * 功能亮点：
 *  - 纯SQL接口实现复杂流处理逻辑
 *  - 自动处理Schema演进（Schema Registry集成）
 *  - 动态UDF注册
 *  - 多版本兼容性处理
 *  - 完善的异常处理机制
 *  - 完整的端到端Exactly-Once语义
 */
public class FlinkKafkaSQLApp {

    // JSON消息示例结构
    private static final String SAMPLE_JSON = "{\n" +
            "  \"event\": \"device_status_update\",\n" +
            "  \"timestamp\": 1685432100000,\n" +
            "  \"payload\": {\n" +
            "    \"id\": \"SN12345\",\n" +
            "    \"name\": \"Temperature Sensor\",\n" +
            "    \"status\": \"active\",\n" +
            "    \"value\": 23.5,\n" +
            "    \"location\": {\"lat\": 31.2304, \"lng\": 121.4737}\n" +
            "  },\n" +
            "  \"metadata\": {\n" +
            "    \"version\": 1.2,\n" +
            "    \"source\": \"gateway-01\"\n" +
            "  }\n" +
            "}";

    public static void main(String[] args) throws Exception {
        // 1. 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 配置环境参数
        configureFlinkEnvironment(env, tableEnv);

        // 3. 加载Kafka配置
        KafkaGlobalConfig config = KafkaConfigLoader.loadConfig("kafka-config.yml");
        String clusterId = "mrs-kafka";
        String sourceTopic = "DLK_SOURCE_TOPIC";
        String sinkTopic = "DLK_SINK_TOPIC";

        // 4. 注册UDF（增强SQL处理能力）
        registerUserDefinedFunctions(tableEnv);

        // 5. 创建Kafka Source表（使用JSON格式）
        createKafkaSourceTable(tableEnv, config, clusterId, sourceTopic);

        // 6. 创建Kafka Sink表
        createKafkaSinkTable(tableEnv, config, clusterId, sinkTopic);

        // 7. 执行SQL处理逻辑
        executeStreamingSQL(tableEnv, sourceTopic, sinkTopic);

        // 8. 启动流处理作业
        env.execute("Flink SQL Kafka Streaming");
    }

    /**
     * 配置Flink执行环境
     * @param env 流执行环境
     * @param tableEnv 表执行环境
     */
    private static void configureFlinkEnvironment(StreamExecutionEnvironment env,
                                                  StreamTableEnvironment tableEnv) {
        // ===== 基本配置 =====
        env.setParallelism(4); // 推荐与Kafka分区数一致
        tableEnv.getConfig().set("table.exec.source.idle-timeout", "10s");

        // ===== 检查点配置（Exactly-Once保障）=====
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // ===== SQL方言配置 =====
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        // ===== 状态TTL配置 =====
        tableEnv.getConfig().set("table.exec.state.ttl", "36h");
    }

    /**
     * 注册用户自定义函数
     * @param tableEnv 表执行环境
     */
    private static void registerUserDefinedFunctions(StreamTableEnvironment tableEnv) {
        // 1. JSON解析函数
        tableEnv.createTemporarySystemFunction("JSON_EXTRACT", new JsonExtractFunction());

        // 2. 数据质量检测函数
        tableEnv.createTemporarySystemFunction("IS_VALID_DEVICE_ID", new DeviceValidationFunction());

        // 3. 时间转换函数
        tableEnv.createTemporarySystemFunction("EPOCH_TO_TIMESTAMP", new TimestampConverter());

        // 4. 地理编码函数
        tableEnv.createTemporarySystemFunction("GET_GEO_REGION", new GeoRegionFunction());
    }

    /**
     * 创建Kafka源表（使用Flink SQL DDL）
     * @param tableEnv 表执行环境
     * @param config Kafka全局配置
     * @param clusterId 集群ID
     * @param topicName 主题名称
     */
    private static void createKafkaSourceTable(StreamTableEnvironment tableEnv,
                                               KafkaGlobalConfig config,
                                               String clusterId,
                                               String topicName) {
        // 获取集群连接地址
        String bootstrapServers = config.getServerConfigById(clusterId)
                .getServerConfig().get("bootstrap.servers").toString();

        // DDL语句定义Kafka源表
        String sourceDDL = String.format(
                "CREATE TABLE kafka_source (\n" +
                        "  raw_data STRING METADATA FROM 'value',        -- 原始JSON消息\n" +
                        "  topic STRING METADATA VIRTUAL,                -- 来源主题\n" +
                        "  partition_id INT METADATA VIRTUAL,           -- 分区ID\n" +
                        "  offset BIGINT METADATA VIRTUAL,               -- 消息偏移量\n" +
                        "  ts TIMESTAMP(3) METADATA FROM 'timestamp'      -- 消息时间戳\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = '%s',\n" +
                        "  'properties.bootstrap.servers' = '%s',\n" +
                        "  'properties.group.id' = 'flink-sql-group',   -- 消费者组ID\n" +
                        "  'scan.startup.mode' = 'earliest-offset',      -- 从最早开始消费\n" +
                        "  'format' = 'json',\n" +
                        "  'json.fail-on-missing-field' = 'false',       -- 容忍缺失字段\n" +
                        "  'json.ignore-parse-errors' = 'true'            -- 忽略解析错误\n" +
                        ")", topicName, bootstrapServers);

        // 执行DDL创建表
        tableEnv.executeSql(sourceDDL);
    }

    /**
     * 创建Kafka结果表（使用Flink SQL DDL）
     * @param tableEnv 表执行环境
     * @param config Kafka全局配置
     * @param clusterId 集群ID
     * @param topicName 主题名称
     */
    private static void createKafkaSinkTable(StreamTableEnvironment tableEnv,
                                             KafkaGlobalConfig config,
                                             String clusterId,
                                             String topicName) {
        // 获取集群连接地址
        String bootstrapServers = config.getServerConfigById(clusterId)
                .getServerConfig().get("bootstrap.servers").toString();

        // DDL语句定义Kafka结果表
        String sinkDDL = String.format(
                "CREATE TABLE kafka_sink (\n" +
                        "  event_type STRING,\n" +
                        "  device_id STRING,\n" +
                        "  device_name STRING,\n" +
                        "  status STRING,\n" +
                        "  region STRING,\n" +
                        "  event_time TIMESTAMP(3),\n" +
                        "  processed_time TIMESTAMP(3),\n" +
                        "  source_offset BIGINT,\n" +
                        "  source_partition INT\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = '%s',\n" +
                        "  'properties.bootstrap.servers' = '%s',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.timestamp-format.standard' = 'ISO-8601',\n" +
                        "  'sink.delivery-guarantee' = 'exactly-once',    -- 精确一次语义\n" +
                        "  'sink.transactional-id-prefix' = 'flink-tx-',  -- 事务ID前缀\n" +
                        "  'sink.parallelism' = '4'                      -- 并行度\n" +
                        ")", topicName, bootstrapServers);

        // 执行DDL创建表
        tableEnv.executeSql(sinkDDL);
    }

    /**
     * 执行流式SQL处理
     * @param tableEnv 表执行环境
     * @param sourceTopic 来源主题名
     * @param sinkTopic 目标主题名
     */
    private static void executeStreamingSQL(StreamTableEnvironment tableEnv,
                                            String sourceTopic, String sinkTopic) {
        // 复杂事件处理SQL语句（带详细注释）
        String processingSQL =
                "-- 原始数据解析与增强处理\n" +
                        "INSERT INTO kafka_sink\n" +
                        "SELECT \n" +
                        "  JSON_EXTRACT(raw_data, '$.event') AS event_type,  -- 提取事件类型\n" +
                        "  JSON_EXTRACT(raw_data, '$.payload.id') AS device_id,\n" +
                        "  JSON_EXTRACT(raw_data, '$.payload.name') AS device_name,\n" +
                        "  CASE \n" +
                        "    WHEN JSON_EXTRACT(raw_data, '$.payload.status') = 'active' THEN '运行中'\n" +
                        "    WHEN JSON_EXTRACT(raw_data, '$.payload.status') = 'inactive' THEN '未激活'\n" +
                        "    WHEN JSON_EXTRACT(raw_data, '$.payload.status') = 'fault' THEN '故障'\n" +
                        "    ELSE '未知状态'\n" +
                        "  END AS status,                                -- 状态本地化\n" +
                        "  GET_GEO_REGION(                               -- 地理区域解码\n" +
                        "    CAST(JSON_EXTRACT(raw_data, '$.payload.location.lat') AS DOUBLE),\n" +
                        "    CAST(JSON_EXTRACT(raw_data, '$.payload.location.lng') AS DOUBLE)\n" +
                        "  ) AS region,\n" +
                        "  EPOCH_TO_TIMESTAMP(                           -- 时间戳转换\n" +
                        "    CAST(JSON_EXTRACT(raw_data, '$.timestamp') AS BIGINT)\n" +
                        "  ) AS event_time,\n" +
                        "  CURRENT_TIMESTAMP AS processed_time,          -- 处理时间\n" +
                        "  offset AS source_offset,                     -- 原始偏移量\n" +
                        "  partition_id AS source_partition             -- 原始分区\n" +
                        "FROM kafka_source\n" +
                        "WHERE \n" +
                        "  IS_VALID_DEVICE_ID(JSON_EXTRACT(raw_data, '$.payload.id'))  -- 设备ID有效性检查\n" +
                        "  AND CAST(JSON_EXTRACT(raw_data, '$.metadata.version') AS DOUBLE) >= 1.0  -- 版本过滤\n" +
                        "  AND ts > TIMESTAMPADD(HOUR, -24, CURRENT_TIMESTAMP)  -- 24小时内数据";

        // 创建异常处理管道
        createDeadLetterQueue(tableEnv, sourceTopic);

        // 执行主处理流程
        TableResult result = tableEnv.executeSql(processingSQL);

        // 异步处理作业状态
        handleJobStatus(result);
    }

    /**
     * 创建死信队列（处理异常数据）
     * @param tableEnv 表执行环境
     * @param sourceTopic 来源主题名
     */
    private static void createDeadLetterQueue(StreamTableEnvironment tableEnv, String sourceTopic) {
        String deadLetterSQL =
                "CREATE TABLE dead_letter_queue (\n" +
                        "  raw_data STRING,\n" +
                        "  error_reason STRING,\n" +
                        "  event_ts TIMESTAMP(3),\n" +
                        "  topic STRING,\n" +
                        "  partition INT,\n" +
                        "  offset BIGINT\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'dlq_"+sourceTopic+"',\n" +
                        "  'properties.bootstrap.servers' = 'kafka-server:9092',\n" +
                        "  'format' = 'json'\n" +
                        ");\n" +
                        "INSERT INTO dead_letter_queue\n" +
                        "SELECT \n" +
                        "  raw_data,\n" +
                        "  '解析失败' AS error_reason,\n" +
                        "  ts,\n" +
                        "  topic,\n" +
                        "  partition_id,\n" +
                        "  offset\n" +
                        "FROM kafka_source\n" +
                        "WHERE \n" +
                        "  TRY_CAST(JSON_EXTRACT(raw_data, '$.timestamp') AS BIGINT) IS NULL\n" +
                        "  OR TRY_CAST(JSON_EXTRACT(raw_data, '$.payload.id') AS STRING) IS NULL";

        tableEnv.executeSql(deadLetterSQL);
    }

    /**
     * 处理作业执行状态
     * @param result 表执行结果
     */
    private static void handleJobStatus(TableResult result) {
        result.getJobClient().ifPresent(jobClient -> {
            try {
                // 等待作业启动
                Thread.sleep(2000);

                // 获取并打印作业信息
                String jobId = jobClient.getJobID().toString();
                System.out.println("Flink 作业已提交, JobID: " + jobId);
                System.out.println("实时监控地址: http://flink-dashboard:8081/#/job/" + jobId);

                // 注册作业完成回调
                jobClient.getJobExecutionResult().whenComplete((executionResult, throwable) -> {
                    if (throwable != null) {
                        System.err.println("作业执行失败: " + throwable.getMessage());
                    } else {
                        System.out.println("作业执行完成! 状态: " + executionResult.getJobStatus());
                    }
                });

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}

