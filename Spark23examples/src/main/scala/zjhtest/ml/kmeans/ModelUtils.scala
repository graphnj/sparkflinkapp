package zjhtest.ml.kmeans

import java.io._





/**
  * @auth zhujinhua 0049003202
  * @date 2019/11/27 17:41
  */

object ModelUtils {


  def saveModel[T](in: T, path:String) {
    val bos = new FileOutputStream(path)
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(in)
    oos.close()
  }



  def loadModel[T](path:String): T = {
    val bis = new FileInputStream(path)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  def saveTxt(in:Array[Array[java.lang.Float]],path:String)={
    val writer = new PrintWriter(new File(path ))
    val str=in.deep.mkString(",").replaceAll("\\),","\\)\r\n")
    writer.write(str)
    writer.close()


  }


  def main(args: Array[String]): Unit = {

  }
}
