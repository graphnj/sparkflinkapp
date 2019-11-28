package zjhtest

import java.lang.ref.WeakReference

import org.apache.spark.{Dependency, ShuffleDependency, SparkConf, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * @auth zhujinhua 0049003202
  * @date 2019/10/21 11:44
  */

object JobTest {

  import zjhtest.metric.ZNVMetrics


  var spark:SparkSession = null


  // ------ Helper functions ------

  protected def newRDD(upval:Int) = spark.sparkContext.makeRDD(1 to upval)
  protected def newPairRDD(upval:Int) = newRDD(upval).map(_ -> 1)
  protected def newShuffleRDD(upval:Int) = newPairRDD(upval).reduceByKey(_ + _)
  protected def newBroadcast() = spark.sparkContext.broadcast(1 to 100)

  protected def newRDDWithShuffleDependencies(): (RDD[_], Seq[ShuffleDependency[_, _, _]]) = {
    def getAllDependencies(rdd: RDD[_]): Seq[Dependency[_]] = {
      rdd.dependencies ++ rdd.dependencies.flatMap { dep =>
        getAllDependencies(dep.rdd)
      }
    }
    val rdd = newShuffleRDD(100)

    // Get all the shuffle dependencies
    val shuffleDeps = getAllDependencies(rdd)
      .filter(_.isInstanceOf[ShuffleDependency[_, _, _]])
      .map(_.asInstanceOf[ShuffleDependency[_, _, _]])
    (rdd, shuffleDeps)
  }



  /** Run GC and make sure it actually has run */
  protected def runGC() {
    val weakRef = new WeakReference(new Object())
    val startTime = System.currentTimeMillis
    System.gc() // Make a best effort to run the garbage collection. It *usually* runs GC.
    // Wait until a weak reference object has been GCed
    while (System.currentTimeMillis - startTime < 10000 && weakRef.get != null) {
      System.gc()
      Thread.sleep(200)
    }
  }


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
