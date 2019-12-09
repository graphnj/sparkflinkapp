package zjhtest.ssckafkaes

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import java.util.{Collections, Properties}

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConversions._

/**
 * @auth zhujinhua 0049003202
 * @date 2019/12/5 15:45
 */

object KafkaTopic {
  def output(rdd: RDD[mutable.Map[String, Any]], fieldsAndtypes: Seq[(String, String)], properties: Properties, topic: String): Unit = {
   // val writer = new RDDKafkaWriterWithTopic[mutable.Map[String, Any]](rdd, fieldsAndtypes, topic)
   // writer.writeToKafka(properties, map2record)
  }

  def map2record(data: collection.Map[String, Any], fieldsAndtypes: Seq[(String, String)], topic: String): ProducerRecord[String, JSONObject] = {
    val record = new JSONObject()
    for((fieldName, fieldType) <- fieldsAndtypes) {
      record.put(fieldName.toLowerCase, data.get(fieldName).get)
    }
    new ProducerRecord[String, JSONObject](topic, null, record)
  }




  def cunsumer(broker:String): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", broker)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")
    props.put("auto.offset.reset","earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("test_v1"))
    while (true){
      val records = consumer.poll(100)
      for (record <- records){
        println(record.offset() +"--" +record.key() +"--" +record.value())
      }
    }
    consumer.close()
  }


  def producer(brokers:String, topics:String): Unit ={

    val topic = "jason_20180511"
    val properties = new Properties()
    properties.put("group.id", "jaosn_")
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName) //key的序列化;
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)//value的序列化;
    val producer = new KafkaProducer[String, String](properties)
    var num = 0
    for(i<- 1 to 1000){
      val json = new JSONObject()
      json.put("name","jason"+i)
      json.put("addr","25"+i)
      producer.send(new ProducerRecord(topics,json.toString()))
    }
    producer.close()
  }


}
