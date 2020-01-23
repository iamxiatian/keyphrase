package wiki.dig.algorithm

import java.nio.charset.StandardCharsets

import better.files.File
import breeze.stats.distributions.Rand
import org.joda.time.DateTime
import wiki.dig.store.db.{CategoryDb, CategoryHierarchyDb}

import scala.collection.mutable
import scala.util.{Random, Success, Try}

/**
  * 生成训练用数据集，每个数据集C由一个子图、子图所拥有的文章构成。
  *
  * 每次生成一个子图后，子图的信息通过父子节点对(父节点, 子节点）的形式保存到文本文件中。
  * 然后生成文档集合，根据子图节点所拥有的文章数量进行有重复的放回抽样得到。每篇文章会
  * 记录其在子图中的路径，同一文章可能会拥有不同的路径。
  */
object CorpusGenerator {
  def generate(startIndex: Int, endIndex: Int, corpusFile: String = "./corpus.txt"): Unit = {
    val writer = File(corpusFile).newBufferedWriter(StandardCharsets.UTF_8)
    var idx = startIndex

    while (idx <= endIndex) {
      Try {
        generateOne(idx) match {
          case Some(text) =>
            writer.write(text)
            writer.write("\n")
            idx += 1
          case None =>
          //println("skip ... ")
        }
      } match {
        case Success(_) =>
          if (idx % 100 == 0) {
            writer.flush()
            println(s"process $idx / $endIndex ... ")
          }
        case scala.util.Failure(e) =>
          e.printStackTrace()
      }
    }
    writer.close()
    println("DONE!")
  }

  /**
    * 生成一个子图，返回一个字符串记录该子图的所有信息，格式如下：
    * 子图id #子图编号 \t 子图的节点集合 \t 子图的边集合 \t 子图的所有文章及路径 \t 子图抽出的文章及路径
    * #1 \t #1 node1, node2 ... node_n \t  n1-n2, n1-n3,  ....    \t  doc1_n1,n2,n3;doc2_n1,n2,n3 .... \t docid_n1,n2,n3;docid_n2,n3,n4...
    *
    */
  def generateOne(corpusId: Int): Option[String] = {
    //生成一个均值为50，标准差为10的高斯分布
    val g = breeze.stats.distributions.Gaussian(30, 5)

    val graph = SubGraphGenerator.generate(g.sample().toInt).toSeq
    //    SubGraphGenerator.toDotFile(graph, "/tmp/test.dot")
    //    Runtime.getRuntime.exec(s"dot -Tpng /tmp/test.dot -o /tmp/test.png")

    //存储父节点的索引信息
    val parentIndex: Map[Int, Seq[Int]] = graph.groupBy(_._2) //按照孩子分组
      .map {
      case (child, parents) =>
        //转换为(孩子节点, 父节点集合)
        (child, parents.map(_._1))
    }

    //生成一个均值为1000，标准差为100的高斯分布
    val g2 = breeze.stats.distributions.Gaussian(1000, 100)

    sampleArticles(graph, parentIndex, g2.sample().toInt) match {
      case Some(sampleDocs) =>
        val sampledDocText = sampleDocs.map {
          case (id, path) =>
            s"${id}_$path"
        }.distinct.mkString(";")

        val nodeText = graph.flatMap { p => Seq(p._1, p._2) }.distinct.mkString(",")
        val edgeText = graph.map {
          case (f, t) => s"$f-$t"
        }.mkString(",")

        val docIds: Set[String] = graph.flatMap { p => Seq(p._1, p._2) }
          .distinct.toSet.flatMap {
          cid: Int =>
            CategoryDb.getPages(cid).map {
              aid: Int =>
                val path = getPath(cid, parentIndex).mkString(",")
                s"${aid}_$path"
            }
        }

        val docIdText = docIds.mkString(";")

        //#1 \t node1, node2 ... node_n \t  n1-n2, n1-n3,  ....    \t  doc1, doc2 .... \t docid_n1,n2,n3; docid_n2,n3,n4...
        Option(s"#${corpusId}\t${nodeText}\t${edgeText}\t${docIdText}\t${sampledDocText}")
      case None =>
        SubGraphGenerator.toDotFile(graph, s"${DateTime.now().toString("yyyyMMddHHmmss")}.dot")
        Option.empty[String]
    }
  }

  /**
    * 抽样子图中的文章, 返回文章与对应路径的列表
    */
  def sampleArticles(graph: Seq[(Int, Int)],
                     parentIndex: Map[Int, Seq[Int]],
                     sampleSize: Int): Option[Seq[(Int, String)]] = {
    val nodeIds = graph.flatMap { p => Seq(p._1, p._2) }.distinct

    //分类节点上的文章数量
    val counts: Seq[Int] = nodeIds.map {
      id =>
        if (CategoryHierarchyDb.startNodeIds.contains(id))
          0
        else {
          //println(s"==> ${CategoryDb.getNameById(id).get}: ${CategoryDb.getPageCount(id).getOrElse(0)}")
          val depth = CategoryHierarchyDb.getCNode(id).get.depth

          //深度越大越容易选中
          if (depth > 0)
            CategoryDb.getPageCount(id).getOrElse(0) * depth
          else
            CategoryDb.getPageCount(id).getOrElse(0)
        }
    }

    val totalArticles = counts.sum
    if (totalArticles < 100) {
      println(s"skip this generated corpus: total articles: $totalArticles")
      None
    } else {
      //子图抽取的文章数量分布，主键为子图上的节点ID，值为该节点上需要抽取的文章数量
      val pickedDist = sampleAllocation(nodeIds, counts, sampleSize)

      val pickedArticles: Seq[(Int, String)] = pickedDist.toSeq.flatMap {
        case (cid, count) =>
          //从节点id中抽文章
          val pickedIds = (1 to count) map {
            _ =>
              val pageIds = CategoryDb.getPages(cid)
              Rand.choose(pageIds).get()
          }

          pickedIds.map {
            id =>
              //val path = getPath(cid, parentIndex).map(CategoryDb.getNameById(_).get)
              val path = getPath(cid, parentIndex)
              (id, path.mkString(","))
          }
      }

      Some(pickedArticles)
    }
  }


  /**
    * 根据子图节点及每个节点的文章数量，抽取一定数量的文章，把每个节点应抽取的文章数量
    * 以Map返回
    */
  private def sampleAllocation(nodeIds: Seq[Int],
                               articleCounts: Seq[Int],
                               sampleSize: Int
                              ): Map[Int, Int] = {
    //子图抽取的文章数量分布，主键为子图上的节点ID，值为该节点上需要抽取的文章数量
    val pickedDist = mutable.Map.empty[Int, Int]
    //val totalArticles: Long = articleCounts.foldLeft(0L)(_ + _)
    val totalArticles: Int = articleCounts.sum

    //设置抽样的文章所在的节点，保存在pickedDist中
    (1 to sampleSize) foreach { _ =>
      //该子图所拥有的文章总数量
      val randNumber = Random.nextInt(totalArticles)
      var accumulator = 0

      //选中的子图节点
      val pickedId: Int = nodeIds.zip(articleCounts).find {
        case (id, c) =>
          if (c + accumulator > randNumber)
            true
          else {
            accumulator += c
            false
          }
      }.map(_._1).getOrElse(nodeIds.last)

      //pickedId的计数器加1
      pickedDist(pickedId) = pickedDist.getOrElse(pickedId, 0) + 1
    }

    pickedDist.toMap
  }

  /**
    * 获取子类的完整路径
    */
  private def getPath(cid: Int, parentIndex: Map[Int, Seq[Int]]): Seq[Int] =
    parentIndex.get(cid) match {
      case Some(parents) =>
        val parentId = Rand.choose(parents).get()
        getPath(parentId, parentIndex) :+ cid
      case None => Seq(cid)
    }

  def main(args: Array[String]): Unit = {
//    println("RUN: bin/corpus-generator <start-index(included> <end-index(included)> <filename>")
//    println("E.g.: bin/corpus-generator 1 10000 corpus.txt")
//
//    if (args.length == 3) {
//      val startIndex = args(0).toInt
//      val sampleCount = args(1).toInt
//      generate(startIndex, sampleCount, args(2))
//    } else {
//      println("Wrong parameters.")
//    }

    val pairs = SubGraphGenerator.generate(30)
    SubGraphGenerator.toDotFile(pairs.toSeq, "/tmp/test.dot")
  }
}
