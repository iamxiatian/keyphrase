package wiki.dig.algorithm.tfidf

import scala.collection.mutable

/**
  * TfIdf统计处理
  */
class TfIdf {
//  //词项与倒排记录表的集合
//  val indexes = mutable.Map.empty[String, Postings]
//
//  def index(doc: IndexDoc): Unit = {
//    doc.termGroup foreach {
//      case (term, cnt) =>
//        index(doc.idx, term, cnt)
//    }
//  }
//
//  def index(docId: Int, term: String, tf: Int): Unit = {
//    val postings: Postings = if (indexes.contains(term)) {
//      indexes(term)
//    } else {
//      val postings = new Postings
//      indexes.put(term, postings)
//      postings
//    }
//
//    postings.incTf(docId, tf)
//  }
//
//  def show(): Unit = {
//    indexes foreach {
//      case (term, postings) =>
//        print(s"$term (${postings.df}): ")
//        println(postings.tf
//          .map {
//            case (docId, tf) =>
//              s"${docId}/$tf"
//          }.mkString(" "))
//    }
//  }
}