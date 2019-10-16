package zjhtest

import java.lang.ref.WeakReference
import java.sql.Time
import java.util.{Random, Timer}

import org.apache.spark.rdd.{RDD, ReliableRDDCheckpointData}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.BroadcastBlockId
import org.apache.spark.util.Utils
import org.apache.spark._
import org.apache.spark.sql.SparkSession



object  CleanerTester
{
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("ContextCleanerSuite")
    .set("spark.cleaner.referenceTracking.blocking", "true")
    .set("spark.cleaner.referenceTracking.blocking.shuffle", "true")
    .set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")


  val spark = SparkSession
    .builder
    .appName("GroupBy Test")
    .config(conf)
    .getOrCreate()



  // ------ Helper functions ------

  protected def newRDD() = spark.sparkContext.makeRDD(1 to 10)
  protected def newPairRDD() = newRDD().map(_ -> 1)
  protected def newShuffleRDD() = newPairRDD().reduceByKey(_ + _)
  protected def newBroadcast() = spark.sparkContext.broadcast(1 to 100)

  protected def newRDDWithShuffleDependencies(): (RDD[_], Seq[ShuffleDependency[_, _, _]]) = {
    def getAllDependencies(rdd: RDD[_]): Seq[Dependency[_]] = {
      rdd.dependencies ++ rdd.dependencies.flatMap { dep =>
        getAllDependencies(dep.rdd)
      }
    }
    val rdd = newShuffleRDD()

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
    var rdd1 = spark.sparkContext.parallelize(0 until 1000, 2)
    // Enforce that everything has been calculated and in cache
    rdd1.checkpoint()
    var rdd2=rdd1.map(_+1).map(_*2)
    val cnt=rdd2.count()
    println("count="+cnt)
    rdd2.checkpoint()
    rdd1.foreach(println)
    rdd1 = spark.sparkContext.parallelize(0 until 10, 2)
    rdd1.checkpoint()
    rdd1.count()
  }

  def main(args: Array[String]) {

    spark.sparkContext.setCheckpointDir("file:///ckpttmp")



    fun()

    runGC()
    var bb=2+1

    Thread.sleep(10000000)


    //pairs1=spark.sparkContext.parallelize(0 until 2, numMappers)
    //println(pairs1.collect())
    spark.stop()
  }
}
