package zjhtest.ml.kmeans

import java.io.FileNotFoundException
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Base64, Calendar, Date}

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark

/**
  * @auth zhujinhua 0049003202
  * @date 2019/11/27 14:35
  */
object ESReader {
  private val startTime = Timestamp.valueOf("2010-01-01 00:00:00")
  private val currentTime = new Timestamp(System.currentTimeMillis())



  def loadESData(sc:SparkContext, indexWithType: String, querystr: String, startTime: Timestamp = startTime,
               endTime: Timestamp = currentTime,featureFieldName:String="feature"): RDD[(String,String,Vector)] = {

    //    ConfigurationManager.loadAndconvertConfigurationFile(true)
    //    ParameterInitializer.initializeAllParameters()


    var query =
      s"""{"query":{
         |"bool":{
         |"must":[{
         |"range":{
         |"enter_time":{ "gte": "${TimeToStr(startTime)}", "lte": "${TimeToStr(endTime)}","time_zone":"+08:00"}
         |}}]}}}""".stripMargin

    if (querystr != null) {
      query = querystr
    }else{
		println("query:"+query)
	}





    val ee = EsSpark.esJsonRDD(sc, indexWithType, query)
    //    ee.take(5).foreach(println)
    val esRdd = ee.map{row => row._2}
        .map(rec => {
          val ele = JSON.parseObject(rec)
          var rt_reature= ele.getString(featureFieldName)

          var feature:Array[Float]=null
          var dimension:Int=256
          val uuid=ele.getString("uuid")
          val enter_time=ele.getString("enter_time")
          if(rt_reature != null) {
            feature = FeatureTools.arrayByte2arrayFloat(Base64.getDecoder.decode(rt_reature))
            dimension = feature.length
            ele.remove(featureFieldName)
          }
          else {
            println("cannot load es field:"+featureFieldName)
          }

          (uuid,enter_time,Vectors.dense(FeatureTools.normalizeFeatrue(feature, dimension).map{_.toDouble}))
        })

    esRdd
  }

  //将字符串类型 types:yyyy-MM-dd HH:mm:ss 转 Date类型
  def strToDate(str: String): Date = {
    val sdf = dateFormat()
    val date: Date = sdf.parse(str)
    date
  }

  //将Date类型转为字符串类型 types:yyyy-MM-dd HH:mm:ss,并转换时区
  def changeDateToStr(date: Date, types: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val sdf = new SimpleDateFormat(types)
    //sdf.setTimeZone(TimeZone.getTimeZone("UTC")) //转换UTC时区
    val str = sdf.format(date)
    str
  }

  //根据结束时间确定开始时间
  def endToStart(endTime: String, hour: Int): String = {
    val sdf = dateFormat()
    val date = sdf.parse(endTime)
    val rightNow = Calendar.getInstance()
    rightNow.setTime(date)
    rightNow.add(Calendar.HOUR, hour)
    val reStr = sdf.format(rightNow.getTime)
    reStr
  }

  //将日期进行按时、天、月、年加减
  def transferDateStr(date: Date, hour: Int, day: Int, month: Int, year: Int): String = {
    val sdf = dateFormat()
    val rightNow = Calendar.getInstance()
    rightNow.setTime(date)
    rightNow.add(Calendar.HOUR, hour)
    rightNow.add(Calendar.DATE, day)
    rightNow.add(Calendar.MONTH, month)
    rightNow.add(Calendar.YEAR, year)
    val reStr = sdf.format(rightNow.getTime)
    reStr
  }

  //计算两个日期的天数间隔  输入两个yyyy-mm-dd,返回天数
  def getIntervalHour(start_date: String, end_date: String) = {
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var begin: Date = df.parse(start_date)
    var end: Date = df.parse(end_date)
    var interval_hour: Long = (end.getTime() - begin.getTime()) / (1000 * 3600) //毫秒转化成秒,然后转化成小时
    val interval_hour_new = interval_hour.toInt
    //      var hour:Float=between.toFloat/3600
    //      var decf:DecimalFormat=new DecimalFormat("#.00")
    //      decf.format(hour)//格式化
    interval_hour_new
  }


  def dateFormat(types: String = "yyyy-MM-dd HH:mm:ss"): SimpleDateFormat = {
    val sdf = new SimpleDateFormat(types)
    sdf
  }

  //将日期字符串转为ES中格式匹配的字符串
  def strToESDate(str: String): String = {
    val es_date = str.replace(" ", "T")
    es_date
  }

  //将str转为 ES Date string
  def TimeToStr(time: Timestamp): String = {
    val f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val str = f.format(time)
    val es_str = str.replace(" ", "T")
    es_str
  }

  def arrayByte2arrayFloat(data: Array[Byte]): Array[Float] = {
    if (data.length <= 12) {
      Array.empty[Float]
    } else {
      val dimCount = (data.length - 12) / 4
      val result = new Array[Float](dimCount)
      var offset = 12
      for (i <- 0 until dimCount) {
        result(i) = java.lang.Float.intBitsToFloat(getInt(data, offset))
        offset += 4
      }
      result
    }
  }

  def getInt(bytes: Array[Byte], offset: Int): Int = {
    (0xff & bytes(offset)) | ((0xff & bytes(offset + 1)) << 8) | ((0xff & bytes(offset + 2)) << 16) | ((0xff & bytes(offset + 3)) << 24)
  }
}
