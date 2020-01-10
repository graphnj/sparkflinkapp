package zjhtest.ml.kmeans



import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.util

import breeze.linalg.DenseMatrix
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @auth zhujinhua 0049003202
  * @date 2019/11/15 23:02
  */
case class InputRow[TId, TVector](id: TId, feature: TVector)
object KMeansTrain {


  val logger = LoggerFactory.getLogger(KMeansTrain.getClass)
  val numClusters:Int = 36
  val numIterations = 1
  val modelPath = "model/"
  val modelOutputFile = modelPath+"model-"+numClusters+".dat"

  // ES config
  val EsServerIp="lv206.dct-znv.com"//10.45.154.206"
  val EsHttpPort=9200
  val indexWithTypeInput="fss_history_yc_without_nullfeature/history_data"
  val featureFieldName = "rt_feature"
  val startTime="2018-06-10 18:00:00"
  val endTime="2018-06-10 19:00:00"



  def loadDataFromES(spark:SparkSession,featureFieldName:String="feature"):DataFrame={


    import spark.implicits._

    val ds1 = ESReader.loadESData(spark.sparkContext, indexWithTypeInput,
      null, Timestamp.valueOf(startTime), Timestamp.valueOf(endTime),
      featureFieldName)
        .map(x=>InputRow(x._1,x._3)).toDF()

    logger.info("load record from ES: "+ds1.count())
    ds1.cache()
  }

  def loadDataFromFile(spark:SparkSession):DataFrame={
    /*val data = sc.textFile("data/mllib/feature11000.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    parsedData.saveAsTextFile("data/mllib/featurevector11000.txt")
    */
    import spark.implicits._
    val parsedData =  MLUtils.loadVectors(spark.sparkContext,"data/mllib/featurevector1000.txt")
    parsedData.map{x=>Vector(x)}.toDF()
  }


  def train(spark:SparkSession,featureFieldName:String="feature")={
    val parsedData = loadDataFromES(spark,featureFieldName:String)

    logger.info("begin train "+numClusters+" cluster and iteration is "+numIterations)
    //val clusters = KMeans.train(parsedData, numClusters, numIterations)
    parsedData.printSchema()
    val kmeans = new KMeans().setK(numClusters).setSeed(1L).setFeaturesCol("feature")
    val model = kmeans.fit(parsedData)
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = model.computeCost(parsedData)
    logger.info(s"Within Set Sum of Squared Errors = $WSSSE")

    model.clusterCenters.foreach(println)

    val arrarr=model.clusterCenters.map(_.toArray.map(_.toFloat).map(Float.box(_)))


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

    val conf = new SparkConf().setAppName("CoarseTrain")

    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", EsServerIp)
    conf.set("es.port", String.valueOf(EsHttpPort))
    conf.set("es.read.field.exclude", "gps_xy") //spark无法读取geo_point类型数据
    //conf.set("es.nodes.wan.only", "false")
    conf.set("es.index.auto.create", "true")
    conf.set("es.index.read.missing.as.empty", "true")

    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    train(spark,featureFieldName)

    sc.stop()
  }
}