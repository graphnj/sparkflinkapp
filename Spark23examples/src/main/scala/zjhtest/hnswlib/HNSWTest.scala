package zjhtest.hnswlib

import java.sql.Timestamp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.slf4j.LoggerFactory
import zjhtest.ml.kmeans.ESReader
import com.github.jelmerk.spark.knn.hnsw.Hnsw
import zjhtest.ml.kmeans.ESReader.TimeToStr


/**
 * @auth zhujinhua 0049003202
 * @date 2019/12/14 21:45
 */
/*
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
$SPARK_COARSETRAIN_JAR_NAME 10.45.154.209:9200 fused_src_data_realtime_yc_8w/fused feature
 */
object HNSWTest {
  case class InputRow[TId, TVector](id: TId, vector: TVector)
  val logger = LoggerFactory.getLogger(HNSWTest.getClass)


  // ES config

  val startTime="2018-06-10 18:00:00"
  val endTime="2018-06-10 23:00:00"


  def loadDataFromES(spark:SparkSession, indexWithTypeInput:String,featureFieldName:String="fused_feature"):DataFrame={


    import spark.implicits._
    var query =
      s"""{"query":{
         |"bool":{
         |"must":[{
         |}]}}}""".stripMargin

    val esdata = ESReader.loadESData(spark.sparkContext, indexWithTypeInput,
      query, Timestamp.valueOf(startTime), Timestamp.valueOf(endTime),featureFieldName)

    esdata.take(10)
      val ds=esdata
        .map(x=>InputRow(x._1,x._3))
        .toDF()
    logger.info("load record from ES: "+ds.count())
    ds.cache()
  }

  def calcIndex(spark:SparkSession, index:String,featureFieldName:String="fused_feature")={
    val parsedData = loadDataFromES(spark,indexWithTypeInput = index,featureFieldName)
    parsedData.printSchema()
    val hnsw = new Hnsw()
      .setIdentityCol("id")
      .setVectorCol("vector")
      .setNumPartitions(20)
      .setM(64)
      .setEf(5)
      .setEfConstruction(200)
      .setK(5)
      .setDistanceFunction("cosine")
      .setExcludeSelf(false)



    val model = hnsw.fit(parsedData)

    logger.info("model.explainParams="+model.explainParams())
    val k=model.transform(parsedData)
    logger.info("model.transform ok =")
    k.show(5)
  }

  def main(args: Array[String]): Unit = {

    val Array(esipport,esindex,featueFieldName) = args

    val EsServerIp=esipport.split(":").apply(0)
    val EsHttpPort=esipport.split(":").apply(1)
   //val indexWithTypeInput="fused_src_data_realtime_yc_8w/fused"
   val indexWithTypeInput=esindex
    val conf = new SparkConf()

    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", EsServerIp)
    conf.set("es.port", String.valueOf(EsHttpPort))
    conf.set("es.read.field.exclude", "gps_xy") //spark无法读取geo_point类型数据
    conf.set("es.nodes.wan.only", "true")
    conf.set("es.index.auto.create", "true")
    conf.set("es.index.read.missing.as.empty", "true")


    conf.getAll.foreach(println)
    println("****conf=",conf)
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    calcIndex(spark,indexWithTypeInput,featueFieldName)
  }
}
