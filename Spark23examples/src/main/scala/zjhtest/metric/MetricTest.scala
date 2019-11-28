package zjhtest.metric

import java.lang.ref.WeakReference

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Dependency, ShuffleDependency, SparkConf, SparkEnv}


/**
  * @auth zhujinhua 0049003202
  * @date 2019/10/21 11:44
  */

object MetricTest {


  var spark:SparkSession = null




  def fun(){


    var rdd1 = spark.sparkContext.makeRDD(0 until 1000, 5)

    var rdd2=rdd1.map(_+1).map(_*2)

    val cnt=rdd2.count()
  println("cnt="+cnt)

    val rdd3=rdd2.map{x=>{{{
      x+1
    x+2}}}}

    //Thread.sleep(10000000)

    val a=5
  }
  import scala.collection.JavaConverters._


  def main(args: Array[String]) {



    System.getProperties.entrySet().asScala.foreach(println(_))
    //  .map(key => (key, System.getProperty(key))).toMap

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("ContextCleanerTester")
      .set("spark.xx","100")
      .set("spark.cleaner.referenceTracking.blocking", "true")
      .set("spark.cleaner.referenceTracking.blocking.shuffle", "false")
      .set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .set("spark.metrics.conf.*.source.jvm.class","org.apache.spark.metrics.source.ZNVMetricsSource")


    spark = SparkSession
      .builder
      .appName("ContextCleanerTester")
      .config(conf)
      .getOrCreate()

    val x=new ZNVMetrics()
    x.func(conf, SparkEnv.get)

    fun()


    spark.stop()
  }
}
