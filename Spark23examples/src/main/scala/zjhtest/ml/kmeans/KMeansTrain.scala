package zjhtest.ml.kmeans



import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import breeze.linalg.DenseMatrix
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.Normalizer
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
  //"yyyy-MM-dd HH:mm:ss"
  val now: Date = new Date()
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("MMddHHmmss")
  val start = dateFormat.format(now)
  val numClusters:Int = 36
  var numIterations = 10
  val modelPath = "model/"
  var modelOutputFile = modelPath+"model"+numClusters+"-"+ start+".dat"
  var modelVectorsTxt = modelPath+"modelvector_" + start + ".txt"
  var modelErrorsTxt = modelPath+"modelerrors_" + start + ".txt"
  var modelGroupCountsTxt = modelPath+"modelcounts_" + start + ".txt"


  // ES config
  var EsServerIp = "10.45.154.206"
  var EsHttpPort = "9200"
  var indexWithTypeInput = "fss_history_yc_without_nullfeature/history_data"
  var featureFile = "data/mllib/featurevector200.txt"
  val featureFieldName = "rt_feature"
  val startTime = "2018-06-10 18:00:00"
  val endTime="2018-06-10 23:00:00"


  /**
   * @auth zhujinhua 0049003202
   * @date 2019/11/15 23:02
   * @date 20200327注：从mllib迁移到ml后此接口未验证
   */
  def loadDataFromES(spark:SparkSession,featureFieldName:String="feature"):DataFrame={


    import spark.implicits._

    val ds1 = ESReader.loadESData(spark.sparkContext, indexWithTypeInput,
      null, Timestamp.valueOf(startTime), Timestamp.valueOf(endTime),
      featureFieldName)
        .map(x=>InputRow(x._1,x._3)).toDF()

    logger.info("load record from ES: "+ds1.count())
    ds1.cache()
  }

  def loadDataFromFile(spark:SparkSession,featuresetfile:String):DataFrame={
    import spark.implicits._
    val parsedData =  MLUtils.loadVectors(spark.sparkContext,featuresetfile)
    // uuid占位，看文件内容后面对应修改
    val y=parsedData.map{x=>InputRow("uuidxx",Vectors.dense(x.toArray))}.toDF()
    y
  }

  def loadData(spark:SparkSession,fromES:Boolean=false,featureFile:String="",esfeatureFieldName:String="feature")=
  {
    var parsedData:DataFrame=null
    if(fromES) {
      parsedData = loadDataFromES(spark, esfeatureFieldName: String)
    } else{
      parsedData = loadDataFromFile(spark,featureFile)
    }
    parsedData.printSchema()
    parsedData
  }

  /**
   * @auth zhujinhua 0049003202
   * @date 2019/11/15 23:02
   * @date 20200327注：采用的余弦距离，需要spark2.4以上，当前采用的是spark2.4.5
   */
  def train(spark:SparkSession, parsedData:DataFrame, featureFieldName:String="feature")={

    logger.info("begin train "+numClusters+" cluster and iteration is "+numIterations)
    //val clusters = KMeans.train(parsedData, numClusters, numIterations)

    val kmeans = new KMeans()
      .setK(numClusters)
      .setSeed(1L)
      .setFeaturesCol("feature")
      .setMaxIter(numIterations)
      .setDistanceMeasure("cosine") //需要spark2.4以上版本，当前使用2.4.5
    val needTrain = true
    if(needTrain) {
      val model = kmeans.fit(parsedData)
      // Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = model.summary.trainingCost //(parsedData)
      logger.info(s"Within Set Sum of Squared Errors = $WSSSE")

      model.clusterCenters.foreach(println)

      val arrarr = model.clusterCenters.map(_.toArray.map(_.toFloat))

      val normarrarr = arrarr.map { x =>
        var sumval = 0.0f
        x.foreach { r => sumval += r * r }
        x.map { r => Float.box(r / sumval) }
      }
      //test
      val test = normarrarr.foreach { x =>
        var sumval = 0.0f
        x.foreach { r => sumval += r * r }
        println(sumval)
      }
      ModelUtils.saveModel(normarrarr, modelOutputFile)

      ModelUtils.saveTxt(normarrarr, modelVectorsTxt)


      // Make predictions
      val predictions = model.transform(parsedData)


      // Evaluate clustering by computing Silhouette score
      val evaluator = new ClusteringEvaluator().setFeaturesCol("feature")

      val silhouette = evaluator.evaluate(predictions)
      println(s"Silhouette with squared euclidean distance = $silhouette")
      //输出Errors到文件
      val writer = new PrintWriter(new File(modelErrorsTxt))
      //聚类后的样本方差
      writer.println(s"Sum of Squared Errors = $silhouette")
      writer.close()

      //model.save("model0327")
    }

    //test code
    //val model2=KMeansModel.load("model0327")
    //val predictions2 = model2.transform(parsedData)

  }


  def main(args: Array[String]) {
    if(args.length!= 4){
      println("Usage: plz add args:[es|file] [esip:port:esindex|filepath] iterationcnt trainindex\n " +
        "like:10.45.154.206:9200:fss_history_yc_without_nullfeature/history_data 200 1")
      //return
    }
    val f1 = new File(modelPath)
    if (!f1.exists)
      f1.mkdirs
    val Array(esorfile,esipportindex_or_path,itercnt,trycnt) = args
    numIterations = itercnt.toInt
    var fromES=true
    // load es info from args
    if(esorfile.equals("es")) {
      val args=esipportindex_or_path.split(":")
      EsServerIp = args.apply(0)
      EsHttpPort =args.apply(1)
      indexWithTypeInput = args.apply(2)
    }
    else{
      fromES=false
      featureFile=esipportindex_or_path
    }
    val suf="-"+start+"-"+trycnt
    modelOutputFile = modelPath+"model"+numClusters+suf+".dat"
    modelVectorsTxt = modelPath+"modelvector" + suf+".txt"
    modelErrorsTxt = modelPath+"modelerrors" + suf+".txt"
    modelGroupCountsTxt = modelPath+"modelcounts" + suf+".txt"

    val conf = new SparkConf().setAppName("CoarseTrain")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", EsServerIp)
    conf.set("es.port", EsHttpPort)
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

    val parsedData = loadData(spark,fromES,featureFile)
    train(spark,parsedData,featureFieldName)

    sc.stop()
  }
}