package wiki.dig.expt

/**
  * 把页面信息从RocksDB中导出到压缩文件中
  */
object ExptMain {

  def main(args: Array[String]): Unit = {
    ExptPageDb.build()
  }
}
