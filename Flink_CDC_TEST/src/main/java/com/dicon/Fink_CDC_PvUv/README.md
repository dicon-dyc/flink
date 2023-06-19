# kafka
bin/kafka-topics.sh --create --bootstrap-server master:9092,slave1:9092,slave2:9092 --topic ods.log_info --replication-factor 2 --partitions 2

bin/kafka-topics.sh --create --bootstrap-server master:9092,slave1:9092,slave2:9092 --topic dwd.pvuv --replication-factor 2 --partitions 2

# mysql
create table if not exists report_user_pvuv( 
window_start timestamp PRIMARY KEY, 
`window_end` timestamp default CURRENT_TIMESTAMP,
pv int, 
uv int )