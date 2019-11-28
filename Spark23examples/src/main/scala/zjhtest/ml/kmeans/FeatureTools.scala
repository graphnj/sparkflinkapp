package zjhtest.ml.kmeans

import com.github.fommil.netlib.BLAS

/**
  * @auth zhujinhua 0049003202
  * @date 2019/11/17 18:50
  */

object FeatureTools {

  private def getInt(bytes: Array[Byte], offset: Int): Int = {
    (0xff & bytes(offset)) | ((0xff & bytes(offset + 1)) << 8) | ((0xff & bytes(offset + 2)) << 16) | ((0xff & bytes(offset + 3)) << 24)
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


  def normalizeFeatrue(feature: Array[Float], dimension: Int): Array[Float] = {
    var normalizedFusedFeature = new Array[Float](dimension)
    val scale = 1.0f / math.sqrt(feature.map(f => f * f).reduce((x, y) => x + y)).toFloat
    axpy(feature, scale, normalizedFusedFeature)
    normalizedFusedFeature
  }

  def axpy(left: Array[Float], right: Float, result: Array[Float]): Unit = {
    val n = left.length
    BLAS.getInstance().saxpy(n, right, left, 1, result, 1)
  }


  def cosDistance(f1: Array[Float], f2: Array[Float], offset: Int): Float = {
    if (f1.length != f2.length) {
      return 0f
    }
    if (f1.length < offset) {
      return 0f
    }
    val dimCnt = f1.length - offset
    var dist = 0.0f
    var d1 = 0.0f
    var d2 = 0.0f
    var i = 0
    while ( {
      i < dimCnt
    }) {
      dist += f1(i) * f2(i)
      d1 += f1(i) * f1(i)
      d2 += f2(i) * f2(i)

      {
        i += 1; i - 1
      }
    }
    val d = Math.sqrt(d1 * d2).toFloat
    dist / d
  }
}
