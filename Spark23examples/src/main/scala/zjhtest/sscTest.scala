package zjhtest

import org.apache.spark.SparkConf
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @auth zhujinhua 0049003202
  * @date 2019/10/28 12:23
  */

object sscTest {

  def main(args: Array[String]) {


    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[4]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream("file:///d:/tmp/aa")

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    println("sdsdsd")
    //wordCounts.print()


    lines.foreachRDD((rdd,batch)=>{
      println(rdd.count())
      rdd.foreach(ll=>{
        println("#:"+ll)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
