package wiki.dig.algorithm.tfidf

import scala.collection.mutable

/**
  * 一个词项对应的倒排记录表信息
  */
class Postings {
  //该词项的文档频度
  def df: Int = docTfMap.size

  //文档id与tf的对应关系
  val docTfMap = mutable.Map.empty[Int, Int]

  /**
    * 按照docID排序输出的所有tf信息
    *
    * @return
    */
  def tf(): Seq[(Int, Int)] = docTfMap.toList.sortWith((l, r) => l._1 < r._1)

  /**
    * 获取文档id对应的词频
    *
    * @param docId
    * @return
    */
  def tf(docId: Int): Int = docTfMap.getOrElse(docId, 0)

  /**
    * 增加文档的词频
    *
    * @param docId
    * @param tfValue
    * @return
    */
  def incTf(docId: Int, tfValue: Int): Postings = {
    docTfMap.put(docId, tf(docId) + tfValue)
    this
  }
}
