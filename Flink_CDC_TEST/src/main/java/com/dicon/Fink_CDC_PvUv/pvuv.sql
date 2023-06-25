--- CDC mysql
CREATE TABLE IF NOT EXISTS `log_info`(
    `id` bigint,
    `user_account` String,
    `login_time` TIMESTAMP(3),
    `click_url` String,
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '192.168.70.48',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'usercontrol',
    'table-name' = 'log_info',
    'scan.incremental.snapshot.enabled' = 'false',
    'scan.startup.mode' = 'latest-offset')

--- kafka ods.log_info
CREATE TABLE IF NOT EXISTS ods_log_info(
    id bigint,
    user_account String,
    login_time TIMESTAMP(3),
    click_url String,
    WATERMARK FOR login_time AS login_time - INTERVAL '5' second
) WITH (
    'connector' = 'kafka',
    'topic' = 'ods.log_info',
    'properties.bootstrap.servers' = 'master:9092,slave1:9092,slave2:9092',
    'sink.partitioner' = 'round-robin',
    'format'='debezium-json',
    'properties.group.id' = 'dwd_user_pvuv_test',
    'scan.startup.mode' = 'earliest-offset')

--- kafka dws.user_pvuv
CREATE TABLE IF NOT EXISTS dwd_user_pvuv(
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    pv bigint,
    uv bigint
) WITH (
    'connector' = 'kafka',
    'topic' = 'dwd.user_pvuv',
    'properties.bootstrap.servers' = 'master:9092,slave1:9092,slave2:9092',
    'format'='debezium-json',
    'properties.group.id' = 'dwd_user_pvuv_test',
    'scan.startup.mode' = 'earliest-offset',
    'sink.partitioner'='round-robin')

--- mysql ads.report_user_pvuv
CREATE TABLE IF NOT EXISTS ads_report_user_pvuv(
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    pv bigint,
    uv bigint,
    PRIMARY KEY(`window_start`) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://192.168.56.104:3306/ads',
    'table-name' = 'report_user_pvuv',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'username' = 'root',
    'password' = '123456' )

--- mysql -> kafka ods -> kafka dws -> mysql ads
INSERT INTO ods_log_info
SELECT id
        ,user_account
        ,login_time
        ,click_url
FROM log_info

--- deprecated kafka ods -> kafka dww pv
INSERT INTO dwd_user_pvuv
SELECT TUMBLE_START(ods_log_info.login_time,INTERVAL '5' SECOND ) AS window_start
        ,TUMBLE_END(ods_log_info.login_time,INTERVAL '5' SECOND ) AS window_end
        ,count(click_url) AS pv
        ,1 AS uv
FROM ods_log_info
GROUP BY TUMBLE(ods_log_info.login_time,INTERVAL '5' SECOND)

--- kafka ods -> kafka dwd pvuv
INSERT INTO dwd_user_pvuv
SELECT TUMBLE_START(ods_log_info.login_time,INTERVAL '1' minute ) AS window_start
     ,TUMBLE_END(ods_log_info.login_time,INTERVAL '1' minute ) AS window_end
     ,count(click_url) AS pv
     ,count(distinct user_account) AS uv
FROM ods_log_info
GROUP BY TUMBLE(ods_log_info.login_time,INTERVAL '1' minute )

--- kafka dws -> mysql ads
INSERT INTO ads_report_user_pvuv
SELECT window_start
        ,window_end
        ,pv
        ,uv
FROM dwd_user_pvuv
