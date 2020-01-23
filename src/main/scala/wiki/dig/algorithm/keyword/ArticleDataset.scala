package wiki.dig.algorithm.keyword

import ruc.irm.extractor.keyword.graph.PositionWordGraph
import wiki.dig.util.{DotFile, Segment}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.xml.{Node, NodeSeq, XML}

/**
  * 关键词抽取的测试文章集合
  */
object ArticleDataset {

  val doc = XML.loadFile("data/articles.xml")

  lazy val articles: NodeSeq = doc \\ "article"

  /** 文章总数量 */
  lazy val articleCount = articles.length

  /** 所有tags标签下包含的标签数量: 3.565 */
  lazy val averageTagCount = (doc \\ "tags").map(_.text.split(",").length).foldLeft(0)(_ + _) / articleCount.toDouble

  /**
    * 文章的平均字符数量： 2629.101
    */
  lazy val averageContentLength = (doc \\ "content").map(_.text.length).foldLeft(0)(_ + _) / articleCount.toDouble

  /**
    * 文章的平均词语数量: 1631.814
    */
  lazy val averageContentWords = (doc \\ "content").map(e => Segment.segment(e.text).length).foldLeft(0)(_ + _) / articleCount.toDouble

  private val acceptPOS = (pos: String) => pos.take(1) == "n" || pos.take(3) == "adj" || pos.take(1) == "v"

  /**
    * 文章的平均词语数量： 过滤后只保留名词，动词和形容词: 757.284
    */
  lazy val filteredAverageContentWords = (doc \\ "content").map(a => Segment.segment(a.text).filter(b => acceptPOS(b._2)).length).foldLeft(0)(_ + _) / articleCount.toDouble

  /**
    * 获取第id篇文章内容
    */
  def getArticle(id: Int): Article = Article(
    (articles(id) \ "url").text,
    (articles(id) \ "title").text,
    (articles(id) \ "tags").text.split(",").map(_.trim).filter(_.nonEmpty),
    (articles(id) \ "content").text
  )

  def getArticle(node: Node): Article = Article(
    (node \ "url").text,
    (node \ "title").text,
    (node \ "tags").text.split(",").map(_.trim).filter(_.nonEmpty),
    (node \ "content").text
  )

  def tags(articleId: Int): String = (articles(articleId) \ "tags").text

  def toDotFile(id: Int, dotFile: String): Unit = {
    val article = ArticleDataset.getArticle(id)
    val g = new PositionWordGraph(1, 0, 0, true)
    g.build(article.title, 30)
    g.build(article.content, 1.0f)

    val pairSet = mutable.Set.empty[String]

    val triples: Seq[(String, String, Int)] = g.getWordNodeMap.asScala.toSeq
      .sortBy(_._2.getCount)(Ordering.Int.reverse)
      .take(100)
      .flatMap {
        case (name, node) =>
          //转换为二元组对：（词语，词语右侧相邻的词语）
          node.getAdjacentWords().asScala.map {
            case (adjName, cnt) =>
              (name, adjName, cnt)
          }
      }

    //过滤掉重复的边
    val triples2 = triples.filter {
      case (first, second, cnt) =>
        if (pairSet.contains(s"${first}_${second}") || pairSet.contains(s"${second}_${first}")) {
          false
        } else {
          pairSet += s"${first}_${second}"
          true
        }
    }

    DotFile.toDotFile(triples2, dotFile)
  }
}

case class Article(url: String,
                   title: String,
                   tags: Seq[String],
                   content: String)
