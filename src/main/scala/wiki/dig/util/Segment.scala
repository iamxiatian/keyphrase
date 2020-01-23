package wiki.dig.util

import better.files.File
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.utility.Predefine

object Segment {
  def init(): Unit = {
//    val hanlpConfigFile = File("./conf/hanlp.properties")
//    println("检查HanLP的属性文件位置")
//    Predefine.HANLP_PROPERTIES_PATH = hanlpConfigFile.canonicalPath
//    if (hanlpConfigFile.exists()) {
//      println(s"\tHanLP分词程序配置文件位置：${hanlpConfigFile.canonicalPath}")
//    } else {
//      println(s"\tHanLP分词程序配置文件位置不存在：${hanlpConfigFile.canonicalPath}")
//    }
  }

  def segment(text: String): List[(String, String)] = {
    import scala.jdk.CollectionConverters._

    HanLP.segment(text).asScala.toList.map {
      term =>
        (term.word, term.nature.toString)
    }
  }

}
