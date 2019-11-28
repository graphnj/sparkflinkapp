package zjhtest.ml.kmeans



import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.util

import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @auth zhujinhua 0049003202
  * @date 2019/11/15 23:02
  */

object KMeansTrain {

  val logger = LoggerFactory.getLogger(KMeansTrain.getClass)
  val numClusters:Int = 36
  val numIterations = 1
  val modelPath = "model/"
  val modelOutputFile = modelPath+"model-"+numClusters+".dat"

  // ES config
  val EsServerIp="10.45.154.207"
  val EsHttpPort=9200
  val indexWithTypeInput="fss_history_yc_without_nullfeature/history_data"
  val startTime="2018-06-10 18:00:00"
  val endTime="2018-06-10 23:00:00"

  val conf = new SparkConf().setAppName("CoarseTrain")

  conf.set("es.index.auto.create", "true")
  conf.set("es.nodes", EsServerIp)
  conf.set("es.port", String.valueOf(EsHttpPort))
  conf.set("es.read.field.exclude", "gps_xy") //spark无法读取geo_point类型数据
  conf.set("es.nodes.wan.only", "true")
  conf.set("es.index.auto.create", "true")
  conf.set("es.index.read.missing.as.empty", "true")

  val sc = new SparkContext(conf)

  def loadDataFromES():RDD[Vector]={


    val ds = ESReader.loadESData(sc, indexWithTypeInput,
      null, Timestamp.valueOf(startTime), Timestamp.valueOf(endTime))

    logger.info("load record from ES: "+ds.count())
    ds.cache()
  }

  def loadDataFromFile():RDD[Vector]={
    val data = sc.textFile("data/mllib/feature11000.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    parsedData.saveAsTextFile("data/mllib/featurevector11000.txt")
    parsedData
  }


  def train()={
    val parsedData = loadDataFromES()

    logger.info("begin train "+numClusters+" cluster and iteration is "+numIterations)
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    logger.info(s"Within Set Sum of Squared Errors = $WSSSE")

    clusters.clusterCenters.foreach(println)

    val arrarr=clusters.clusterCenters.map(_.toArray.map(_.toFloat).map(Float.box(_)))


    ModelUtils.saveModel(arrarr,modelOutputFile)

    ModelUtils.saveTxt(arrarr,modelPath+"model.txt")

    arrarr
  }

  def testLoad()={
//    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel2")
//    //parsedData.foreach{x=>println(sameModel.predict(x))}
//
//    sameModel.clusterCenters.foreach(println(_))
//
//    val arrarr2=sameModel.clusterCenters.map(_.toArray.map(_.toFloat).map(Float.box(_)))
//    serialize(arrarr2,"model.dat")

    val arrarr3:Array[Array[java.lang.Float]]=ModelUtils.loadModel[Array[Array[java.lang.Float]]](modelOutputFile)
    arrarr3.foreach(println(_))
  }
  def main(args: Array[String]) {
    val f1 = new File(modelPath)
    if (!f1.exists)
      f1.mkdirs

    train()

    sc.stop()
  }
}