package wiki.dig.http.route

import java.io.File

import io.circe.syntax._
import org.zhinang.conf.Configuration
import ruc.irm.extractor.keyword.TextRankExtractor
import ruc.irm.extractor.keyword.TextRankExtractor.GraphType
import ruc.irm.extractor.keyword.TextRankExtractor.GraphType.{PositionDivRank, PositionRank}
import ruc.irm.extractor.nlp.SegmentFactory
import spark.Spark._
import spark.{Request, Response, Route}
import wiki.dig.algorithm.keyword.PaperDataset
import wiki.dig.util.Logging

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object PaperRoute extends JsonSupport with Logging {
  val weightedExtractor: TextRankExtractor = new TextRankExtractor(PositionRank)
  //  val ningExtractor: TextRankExtractor = new TextRankExtractor(NingJianfei)
  //  val clusterExtractor: TextRankExtractor = new TextRankExtractor(ClusterRank)

  val weightedDivExtractor: TextRankExtractor = new TextRankExtractor(PositionDivRank)
  //  val clusterDivExtractor: TextRankExtractor = new TextRankExtractor(ClusterDivRank)

  def register(): Unit = {
    //获取账号根据邮箱
    get("/paper/extract.do", "application/json", extract)

    get("/paper/test", "text/html", test)

    get("/paper/graph", "text/html", getWordGraph)

    get("/paper/show", "text/html", show)

    get("/paper/list_keywords", "text/html", listKeywords)

    get("/paper/list_phrases", "text/html", listPhrases)
  }

  /**
    * 评估结果，返回P, R， F
    *
    * @param keywords
    * @param truth
    * @return
    */
  def eval(keywords: Seq[String], truth: Seq[String]): (Double, Double, Double) = {
    val judgedSet = mutable.Set.empty[String] //已经判断过匹配成功的记录
    //    val intersection1 = keywords.count {
    //      k =>
    //        //考虑到短语问题，只要部分匹配，也认为命中
    //        truth.exists {
    //          p =>
    //            val existed = (p.contains(k) || k.contains(p))
    //            if (existed) {
    //              //如果部分匹配，则需要记录到judgeSet中，避免重复匹配
    //              if (judgedSet.contains(p)) false
    //              else {
    //                judgedSet += p
    //                true
    //              }
    //            } else false
    //        }
    //    }

    val intersection1 = keywords.count {
      k =>
        //完整匹配
        truth.exists(_ == k)
    }

    val P = intersection1 * 1.0 / keywords.length
    val R = intersection1 * 1.0 / truth.length
    val F = if (P + R == 0) 0.0 else 2 * P * R / (P + R)
    (P, R, F)
  }

  def getResult(topN: Int, findPhrase: Boolean): String = {
    var macroP1 = 0.0
    var macroR1 = 0.0
    val detail = PaperDataset.papers.zipWithIndex.map {
      case (paper, idx) =>
        println(s"process $idx/${PaperDataset.count()}...")
        val keywords1: Seq[String] = if (findPhrase)
          weightedExtractor.extractPhraseAsList(paper.title, paper.`abstract`, topN).asScala.toSeq
        else
          weightedExtractor.extractAsList(paper.title, paper.`abstract`, topN).asScala.toSeq

        val tags = paper.tags

        val (p1: Double, r1: Double, f1: Double) = eval(keywords1, tags)

        macroP1 += p1
        macroR1 += r1

        //抽取结果中，tags至少包含一个
        val existedOne = keywords1.exists(tags.contains(_))
        val indicator = if (existedOne) "GOOD" else "BAD"
        s"""
           |[$indicator] $idx: <a href="/paper/show?id=$idx" target="_blank">${paper.title}</a><br/>
           |tags: ${paper.tags.map { t => s"<a href='/wiki/page?name=$t' target='_blank'>$t</a>" }.mkString("; ")}<br/>
           |WeightRank: ${keywords1.map { t => s"<a href='/wiki/page?name=$t' target='_blank'>$t</a>" }.mkString("; ")}<br/>
           |P: $p1, R: $r1, F: $f1 <br/>
           |""".stripMargin
    }.mkString("<div>", "\n<hr/>", "</div>")

    macroP1 = macroP1 / PaperDataset.count()
    macroR1 = macroR1 / PaperDataset.count()

    val macroF1 = 2 * macroP1 * macroR1 / (macroP1 + macroR1)

    s"""
       |<h3>WeightRank: macroP: $macroP1, macroR: $macroR1, macroF: $macroF1</h3>
       |$detail
       |""".stripMargin
  }

  def getDivRankResult(topN: Int): String = {
    var macroP1 = 0.0
    var macroR1 = 0.0
    var macroP2 = 0.0
    var macroR2 = 0.0
    val detail = PaperDataset.papers.zipWithIndex.map {
      case (paper, idx) =>
        println(s"process $idx/${PaperDataset.count()}...")
        val keywords1: Seq[String] = weightedExtractor.extractAsList(paper.title, paper.`abstract`, topN).asScala.toSeq
        val keywords2: Seq[String] = weightedDivExtractor.extractAsList(paper.title, paper.`abstract`, topN).asScala.toSeq

        val tags = paper.tags

        val (p1: Double, r1: Double, f1: Double) = eval(keywords1, tags)
        val (p2: Double, r2: Double, f2: Double) = eval(keywords2, tags)

        macroP1 += p1
        macroR1 += r1

        macroP2 += p2
        macroR2 += r2

        //抽取结果中，tags至少包含一个
        val existedOne = keywords1.exists(tags.contains(_))
        val indicator = if (p1 > p2) "OLD" else "NEW"
        s"""
           |[BETTER: $indicator] $idx: <a href="/paper/show?id=$idx" target="_blank">${paper.title}</a><br/>
           |tags: ${paper.tags.map { t => s"<a href='/wiki/page?name=$t' target='_blank'>$t</a>" }.mkString("; ")}<br/>
           |WeightRank: ${keywords1.map { t => s"<a href='/wiki/page?name=$t' target='_blank'>$t</a>" }.mkString("; ")}<br/>
           |P: $p1, R: $r1, F: $f1 <br/>
           |DivRank: ${keywords2.map { t => s"<a href='/wiki/page?name=$t' target='_blank'>$t</a>" }.mkString("; ")}<br/>
           |P: $p2, R: $r2, F: $f2 <br/>
           |""".stripMargin
    }.mkString("<div>", "\n<hr/>", "</div>")

    macroP1 = macroP1 / PaperDataset.count()
    macroR1 = macroR1 / PaperDataset.count()

    macroP2 = macroP2 / PaperDataset.count()
    macroR2 = macroR2 / PaperDataset.count()

    val macroF1 = 2 * macroP1 * macroR1 / (macroP1 + macroR1)
    val macroF2 = 2 * macroP2 * macroR2 / (macroP2 + macroR2)

    s"""
       |<h3>WeightRank: macroP: $macroP1, macroR: $macroR1, macroF: $macroF1</h3>
       |<h3>DivRank: macroP: $macroP2, macroR: $macroR2, macroF: $macroF2</h3>
       |$detail
       |""".stripMargin
  }

  val resultMap = mutable.Map.empty[Int, String]

  /**
    * 抽取完全失败的文章列表
    *
    * @return
    */
  def listKeywords: Route = (request: Request, _: Response) => {
    val topN = Option(request.queryMap("topN").value()).flatMap(_.toIntOption).getOrElse(10)
    if (!resultMap.contains(topN)) {
      resultMap(topN) = ""
      resultMap(topN) = getResult(topN, false)
    }

    resultMap(topN)
  }


  val phraseMap = mutable.Map.empty[Int, String]

  def listPhrases: Route = (request: Request, _: Response) => {
    val topN = Option(request.queryMap("topN").value()).flatMap(_.toIntOption).getOrElse(10)
    if (!phraseMap.contains(topN)) {
      phraseMap(topN) = ""
      phraseMap(topN) = getResult(topN, true)
    }

    phraseMap(topN)
  }

  /**
    * 显示文章内容
    *
    * @return
    */
  def show: Route = (request: Request, _: Response) => {
    val topN = Option(request.queryMap("topN").value()).flatMap(_.toIntOption).getOrElse(5)

    Option(request.queryMap("id").value()).flatMap(_.toIntOption) match {
      case Some(id) =>
        val paper = PaperDataset.get(id)
        val keywords1 = weightedExtractor.extractAsString(paper.title, paper.`abstract`, topN)
        //val keywords2 = weightedDivExtractor.extractAsString(paper.title, paper.`abstract`, topN)

        //显示文本分词后的结果
        val titleWords = SegmentFactory.getSegment(new Configuration()).tag(paper.title).asScala

        val contentWords = SegmentFactory.getSegment(new Configuration()).tag(paper.`abstract`).asScala

        if (!new File(s"./www/dot2/${id}.png").exists()) {
          PaperDataset.toDotFile(id.toInt, s"./www/dot2/${id}.png")
        }

        s"""
           |<html><head><title>${paper.title}</title></head>
           |<body>
           |  <h2>${paper.title}</h2>
           |  <ul>
           |  <li>segment: ${titleWords.map(_.toString).mkString(" ")}</li>
           |  <li>tags: ${paper.tags.mkString("; ")}</li>
           |  <li>WeightRank: ${keywords1}</li>
           |  <li><a href="graph?id=${id}">查看词图</a></li>
           |  </ul>
           |  <div>
           |  ${paper.`abstract`.replaceAll("\n", "<br/>")}
           |  </div>
           |  <hr/>
           |  <div>
           |  ${contentWords.map(_.toString).mkString(" ")}
           |  </div>
           |  <div>
           |    <img src="/dot2/${id}.png"/>
           |  </div>
           |</body></html>
           |""".stripMargin
      case None =>
        "未指定参数id"
    }
  }

  /**
    * 把指定id文章的词图，转成dot文件，方便查看。
    *
    * @return
    */
  def getWordGraph: Route = (request: Request, _: Response) => {
    Option(request.queryMap("id").value()).flatMap(_.toIntOption) match {
      case Some(id) =>
        PaperDataset.toDotFile(id, s"./www/dot2/${id}.dot")
        s"""<a href="/dot2/${id}.dot">下载dot文件</a>
           |<a href="/dot2/${id}.png">下载png文件</a>
           |""".stripMargin
      case None =>
        "未指定参数id"
    }
  }

  /**
    * 测试指定id文章的关键词抽取结果
    *
    * @return
    */
  private def test: Route = (request: Request, _: Response) => {
    val topN = Option(request.queryMap("topN").value()).flatMap(_.toIntOption).getOrElse(5)

    Option(request.queryMap("id").value()).flatMap(_.toIntOption) match {
      case Some(id) =>
        val paper = PaperDataset.get(id)
        val keywords1 = weightedExtractor.extractAsString(paper.title, paper.`abstract`, topN)
        val keywords2 = weightedDivExtractor.extractAsString(paper.title, paper.`abstract`, topN)
        s"""
           |<html>
           |<head><title>测试[id: ${id}]：${paper.title}</title></head>
           |<body>
           |<ul>
           |  <li>Title: ${paper.title}</li>
           |  <li>tags: ${paper.tags.mkString(" ")}</li>
           |  <li>WeightRank: ${keywords1}</li>
           |  <li>DivRank: ${keywords2}</li>
           |</ul>
           |<div>${paper.`abstract`.replaceAll("\n", "<p/>")}</div>
           |</body>
           |</html>
           |""".stripMargin
      case None =>
        s"未指定文章ID"
    }
  }

  private def extract: Route = (request: Request, _: Response) => {
    val title = Option(request.queryMap("title").value()).getOrElse("").trim
    val content = Option(request.queryMap("content").value()).getOrElse("").trim

    val positionRank = new TextRankExtractor(GraphType.PositionRank).extractAsList(title, content, 10)
    jsonOk(positionRank.asScala.map(_.asJson).asJson)
  }
}
