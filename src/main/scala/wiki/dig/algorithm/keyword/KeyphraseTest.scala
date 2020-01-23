package wiki.dig.algorithm.keyword

import wiki.dig.http.route.PaperRoute.weightedExtractor

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object KeyphraseTest {
  def entropy(dist: Seq[Int]): Double = {
    val sum = dist.sum
    val probabilities = dist.map(_ * 1.0 / sum)

    probabilities.map(p => -p * Math.log(p) / Math.log(2)).sum
  }

  def test(id: Int): Unit = {
    val paper = PaperDataset.get(id)
    println(s"${paper.title}:\n\t\t${paper.tags.mkString(";")}")
    val keywords1 = weightedExtractor.extractPhraseAsList(paper.title, paper.`abstract`, 10)
    keywords1.forEach(k => {
      println(k)
    })

  }

  def eval(keywords: Seq[String], truth: Seq[String]): Double = {
    val judgedSet = mutable.Set.empty[String] //已经判断过匹配成功的记录

    val intersection1 = keywords.count {
      k =>
        //完整匹配
        truth.exists(_ == k)
    }

    val P = intersection1 * 1.0 / keywords.length

    P
  }

  def computeAP(keywords: Seq[String], truth: Seq[String]): Double = {
    var apValue = 0.0
    (1 to keywords.length).foreach {
      size =>
        val topList = keywords.take(size)
        if (truth.contains(topList.last))
          apValue += eval(topList, truth)
    }

    apValue / truth.length
  }

  def map(topN: Int, findPhrase: Boolean): Double = {
    var totalAP = 0.0

    PaperDataset.papers.zipWithIndex.map {
      case (paper, idx) =>
        val keywords1: Seq[String] = if (findPhrase)
          weightedExtractor.extractPhraseAsList(paper.title, paper.`abstract`, topN).asScala.toSeq
        else
          weightedExtractor.extractAsList(paper.title, paper.`abstract`, topN).asScala.toSeq

        val tags = paper.tags

        totalAP += computeAP(keywords1, tags)
    }

    val map = totalAP / PaperDataset.count()
    //println(s"topN:$topN, phrase: $findPhrase, map: $map")
    //println(s"$map")
    map
  }

  def rmap(findPhrase: Boolean): Unit = {
    var totalAP = 0.0

    PaperDataset.papers.zipWithIndex.map {
      case (paper, idx) =>
        val tags = paper.tags
        val topN = tags.size

        val keywords1: Seq[String] = if (findPhrase)
          weightedExtractor.extractPhraseAsList(paper.title, paper.`abstract`, topN).asScala.toSeq
        else
          weightedExtractor.extractAsList(paper.title, paper.`abstract`, topN).asScala.toSeq

        totalAP += computeAP(keywords1, tags)
    }

    val map = totalAP / PaperDataset.count()
    println(s"phrase: $findPhrase, R-map: $map")
  }

  /**
    * 显示R-AP=0的文档列表
    *
    */
  def badResult(): Unit = {
    var count = 0
    PaperDataset.papers.zipWithIndex.map {
      case (paper, idx) =>
        val tags = paper.tags
        val topN = tags.size

        val phrases: Seq[String] = weightedExtractor.extractPhraseAsList(paper.title, paper.`abstract`, topN).asScala.toSeq
        val words: Seq[String] = weightedExtractor.extractAsList(paper.title, paper.`abstract`, topN).asScala.toSeq

        val ap = computeAP(phrases, tags)
        if (ap == 0.0) {
          count += 1
          println(s"${paper.title} & ${paper.tags.mkString(", ")} & ${words.mkString(s", ")} & ${phrases.mkString(", ")} \\\\")
        }
    }

    println(s"count: $count")
  }


  def main(args: Array[String]): Unit = {
    println(PaperDataset.statistics())
//    println(s"dataset count: ${PaperDataset.count()}")
//    println("topN Phrase Keyword")
//    (1 to 10).foreach {
//      topN =>
//        val phraseMAP = map(topN, true)
//        val keywordMAP = map(topN, false)
//        println(s"$topN ${phraseMAP.formatted("%.3f")} ${keywordMAP.formatted("%.3f")}")
//    }
//
//    rmap(true)
//    rmap(false)
//
//    badResult()
  }
}
