-- 设置必要的配置
SET 'execution.runtime-mode' = 'streaming';
SET 'table.exec.source.cdc-events-duplicate-behavior' = 'ignore';

-- 创建Kafka Source Table
CREATE TABLE kafka_source (
    app_name STRING,
    time_stamp STRING,
    level STRING,
    logger STRING,
    message STRING,
    thread STRING,
    exception STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'your_kafka_topic',
    'properties.bootstrap.servers' = 'your_kafka_broker:9092',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

-- 创建Hudi Sink Table
CREATE TABLE hudi_sink (
    app_name STRING,
    time_stamp STRING,
    unix_time_stamp BIGINT,
    level STRING,
    logger STRING,
    message STRING,
    thread STRING,
    exception STRING,
    load_dt STRING
) PARTITIONED BY (load_dt)
WITH (
    'connector' = 'hudi',
    'path' = 'hdfs://hacluster/dlkdata/dlk_shark_dev/dlk_sor_dev/rtsor_log_ccdlk_openserver_mor',
    'table.type' = 'MERGE_ON_READ',
    'write.tasks' = '4',
    'hoodie.datasource.write.recordkey.field' = 'app_name,unix_time_stamp',
    'hoodie.datasource.write.precombine.field' = 'unix_time_stamp'
);

INSERT INTO hudi_sink
SELECT 
    app_name,
    time_stamp,
    UNIX_TIMESTAMP(time_stamp, 'yyyy-MM-dd HH:mm:ss.SSS') * 1000 AS unix_time_stamp,
    TRIM(level) AS level,
    logger,
    message,
    thread,
    exception,
    SUBSTRING(time_stamp, 1, 10) AS load_dt
FROM kafka_source;



