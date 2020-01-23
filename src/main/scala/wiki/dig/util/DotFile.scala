package wiki.dig.util

import better.files.File
import wiki.dig.algorithm.keyword.ArticleDataset

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * GraphViz的dot格式的文件生成工具
  */
object DotFile extends Logging {

  case class Edge(src: String, dest: String, weight: Int, style: Int)

  def save(edges: Seq[Edge], dotFile: String): Unit = {
    //出现的所有的名称
    val names = edges.flatMap(t => List(t.src, t.dest)).toSet.toSeq

    // 名称对应的下标作为id
    val ids: Seq[Int] = (1 to names.size)

    //每个名称的度（如果指定了边的权重，则累加权重）,主键为名称，值为度
    val nameWeights: mutable.Map[String, Int] = mutable.Map.empty[String, Int]
    edges.map {
      edge =>
        nameWeights(edge.src) = nameWeights.getOrElse(edge.src, 0) + edge.weight
        nameWeights(edge.dest) = nameWeights.getOrElse(edge.dest, 0) + edge.weight
    }

    //名称和下标的映射
    val nameIdMapping: Map[String, Int] = names.zip(ids).toMap

    val tips: Seq[String] = ids.zip(names).map {
      case (id, name) =>
        s"""$id [label="${name}/${nameWeights.getOrElse(name, 0)}", fontname="FangSong"];"""
    }

    val nodeText = tips.mkString("\n")

    val edgeText = edges.map {
      edge =>
        //设置不同类型的风格
        val edgeStyle = if (edge.style == 1) "color=\"red\"" else "style=\"dashed\", color=\"grey\", dir=\"none\""
        s"""${nameIdMapping.get(edge.src).get} -> ${nameIdMapping.get(edge.dest).get}[$edgeStyle, label="${edge.weight}"];"""
    }.mkString("\n")

    val dotText =
      s"""
         |digraph g {
         |  graph [ordering="out"];
         |  margin=0;
         |  overlap = false;
         |  #splines = "curved";
         |  splines = true;
         |  $nodeText
         |  $edgeText
         |}
    """.stripMargin

    val f = File(dotFile)
    f.parent.createDirectoryIfNotExists(true)

    f.writeText(dotText)
    Try {
      Runtime.getRuntime.exec(s"neato -Tpng ${dotFile} -o ${dotFile.replace(".dot", ".png")}")
    } match {
      case Success(_) =>

      case Failure(exception) =>
        LOG.error("save dot file error", exception)
    }
    // Runtime.getRuntime.exec("dot -Tpdf /tmp/tree.dot -o /tmp/tree.pdf")
    //Runtime.getRuntime.exec(s"dot -Tpng /tmp/tree-$i.dot -o /tmp/test-$i.png")
  }

  /**
    * 将pair数据集，转换为dot语法，输出到dot文件中，方便可视化呈现
    * DOT语法参考Graphviz
    */
  def toDotFile(triples: Seq[(String, String, Int)], dotFile: String): Unit = {
    //出现的所有的名称
    val names = triples.flatMap(t => List(t._1, t._2)).toSet.toSeq

    // 名称对应的下标作为id
    val ids: Seq[Int] = (1 to names.size)

    //每个名称的度（如果指定了边的权重，则累加权重）,主键为名称，值为度
    val nameWeights: mutable.Map[String, Int] = mutable.Map.empty[String, Int]
    triples.map {
      case (from, to, cnt) =>
        nameWeights(from) = nameWeights.getOrElse(from, 0) + cnt
        nameWeights(to) = nameWeights.getOrElse(to, 0) + cnt
    }

    //名称和下标的映射
    val nameIdMapping: Map[String, Int] = names.zip(ids).toMap

    val tips: Seq[String] = ids.zip(names).map {
      case (id, name) =>
        s"""$id [label="${name}/${nameWeights.getOrElse(name, 0)}", fontname="FangSong"];"""
    }

    val nodeText = tips.mkString("\n")
    val edgeText = triples.map {
      case (first, second, cnt) => s"""${nameIdMapping.get(first).get} -> ${nameIdMapping.get(second).get}[label="${cnt}"];"""
    }.mkString("\n")

    //    val dotText =
    //      s"""
    //         |digraph g {
    //         |  graph [ordering="out"];
    //         |  margin=0;
    //         |  $nodeText
    //         |  $edgeText
    //         |}
    //    """.stripMargin

    val dotText =
      s"""
         |digraph g {
         |  graph [ordering="out"];
         |  margin=0;
         |  $nodeText
         |  $edgeText
         |}
    """.stripMargin

    val f = File(dotFile)
    f.parent.createDirectoryIfNotExists(true)

    f.writeText(dotText)
    Try {
      Runtime.getRuntime.exec(s"sfdp -Tpng ${dotFile} -o ${dotFile.replace(".dot", ".png")}")
    } match {
      case Success(_) =>

      case Failure(exception) =>
        LOG.error("save dot file error", exception)
    }
    // Runtime.getRuntime.exec("dot -Tpdf /tmp/tree.dot -o /tmp/tree.pdf")
    //Runtime.getRuntime.exec(s"dot -Tpng /tmp/tree-$i.dot -o /tmp/test-$i.png")
  }

  def main(args: Array[String]): Unit = {
    //toDotFile(Seq(("中国", "人民"), ("中国", "上海"), ("中国", "北京")), "test.dot")
    ArticleDataset.toDotFile(1, "test.dot")

  }
}
