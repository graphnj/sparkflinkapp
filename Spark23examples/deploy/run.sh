#!/bin/bash
set -x
#LOCAL_PATH=`pwd`
LOCAL_PATH=`dirname $0`
LOCAL_PATH=`cd ${LOCAL_PATH};pwd`


BROKERLIST=10.45.157.91:9092
ESIPPORT=10.45.157.91:9200



if [ ! -d "$LOCAL_PATH/log" ]; then
sudo mkdir -p $LOCAL_PATH/log
fi

LOG_DIR=$LOCAL_PATH/log

LOCAL_JAR_PATH=$LOCAL_PATH/lib

SPARK_LAB_JAR_NAME=$LOCAL_JAR_PATH/zjh-spark-lab-1.0.0.jar

spark2-submit \
--class zjhtest.hnswlib.HNSWTest \
--name HNSWTest \
--master yarn \
--deploy-mode client \
--num-executors 5 \
--executor-memory 8g \
--executor-cores 4 \
--driver-memory 4g \
--driver-cores 3 \
--conf spark.streaming.kafka.maxRatePerPartition=10 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer.max=1G \
--conf spark.streaming.stopGracefullyOnShutdown=true \
--conf spark.sql.shuffle.partitions=50 \
--conf spark.streaming.backpressure.enabled=true \
--conf spark.streaming.concurrentJobs=1 \
--conf spark.yarn.max.executor.failures=8000 \
--conf spark.driver.memoryOverhead=512 \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.cleaner.referenceTracking.cleanCheckpoints=true \
--conf spark.locality.wait=3 \
--conf spark.driver.extraJavaOptions="-XX:+UseG1GC -Dlog4j.configuration=log4j.properties" \
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -Dlog4j.configuration=log4j.properties" \
--files $LOCAL_PATH/log4j.properties \
--jars $LOCAL_JAR_PATH/breeze_2.11-0.13.2.jar,\
$LOCAL_JAR_PATH/fastjson-1.2.47.jar,\
$LOCAL_JAR_PATH/hnswlib-core-0.0.27.jar,\
$LOCAL_JAR_PATH/hnswlib-scala_2.11-0.0.27.jar,\
$LOCAL_JAR_PATH/hnswlib-spark_2.3.0_2.11-0.0.27.jar,\
$LOCAL_JAR_PATH/hnswlib-utils-0.0.27.jar,\
$LOCAL_JAR_PATH/elasticsearch-spark-20_2.11-5.4.0.jar,\
$LOCAL_JAR_PATH/eclipse-collections-9.2.0.jar,\
$LOCAL_JAR_PATH/eclipse-collections-api-9.2.0.jar,\
$LOCAL_JAR_PATH/spark-streaming-kafka-0-10_2.11-2.3.0.jar \
$SPARK_LAB_JAR_NAME 10.45.154.209:9200 fused_src_data_realtime_yc_8w/fused feature > $LOG_DIR/train.out 2>&1 &
