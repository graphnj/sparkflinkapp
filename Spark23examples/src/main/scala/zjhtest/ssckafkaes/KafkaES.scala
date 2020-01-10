package zjhtest.ssckafkaes

/**
 * @auth zhujinhua 0049003202
 * @date 2019/12/5 14:39
 */


import java.util.{Date, Properties}
import java.util.concurrent.{Executors, TimeUnit}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.elasticsearch.spark._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2
 */
object KafkaES {

  private def setESConf(conf: SparkConf, esipport:String): Unit = {
    conf.set("es.nodes", esipport.split(":").apply(0))
    conf.set("es.port", esipport.split(":").apply(1))
    conf.set("es.nodes", "lv208.dct-znv.com")
    conf.set("es.port", "9200")
    //conf.set("es.index.auto.create", "true")
    //conf.set("es.input.json","true")
    //conf.set("es.nodes.wan.only", "true")
    //conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
  }
  def sendKafka(brokers:String, topics:String)={


    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable {
      val properties = new Properties()
      properties.put("group.id", "zjhgrpid20191205")
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName) //key的序列化;
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)//value的序列化;
      val producer = new KafkaProducer[String, String](properties)

      override def run(): Unit = {

        var num = 0

        val json = new JSONObject()
        json.put("enter_time",new Date())
        json.put("value", scala.util.Random.nextInt())
        val rst=producer.send(new ProducerRecord(topics,json.toString()))
        //println("send to kafka"+rst.get())
      }
    }, 0, 100, TimeUnit.MILLISECONDS)
  }
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: xx  <brokers> <topics> <esipport> <esindex>
                            |xx 10.45.154.218:9092 testtopic 10.45.154.218:9200 testesindex
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }


    println(args)
    val Array(brokers, topics, esipport,esindex) = args

    sendKafka(brokers, topics)

    println ("brokers:"+brokers)
    println ("topics:"+topics)
    println ("esipport:"+esipport)
    println ("esindex:"+esindex)
    //Thread.sleep(100000000)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaESCheck")
    setESConf(sparkConf, esipport)
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG->brokers,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG->classOf[StringSerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG->classOf[StringSerializer].getName,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer].getName,
      ConsumerConfig.GROUP_ID_CONFIG->"zjhconsumer"
    )


    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))


    messages.foreachRDD{rdd=>
      rdd.foreach(println)
      val data = rdd.map(x => {
        JSON.parse(x.value())
      })
        .saveToEs(esindex+"/zjhestest")
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
