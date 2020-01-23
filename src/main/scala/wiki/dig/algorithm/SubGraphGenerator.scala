package wiki.dig.algorithm

import better.files.File
import wiki.dig.store.db.{CategoryDb, CategoryHierarchyDb}

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.util.Random

/**
  * 生成论文需要的数据集合: 每一个小的语料库(corpus)的抽取策略：
  *
  * 随机挑选一个分类节点，Random Walk到相邻节点，继续Random Walk, 直到选够节点数量为止。
  *
  * First, pick one
  *
  */

/**
  * 随机生成一个子图
  */
object SubGraphGenerator {

  trait Direction

  case object Up extends Direction

  case object Down extends Direction

  case object LeftRight extends Direction

  import wiki.dig.store.db.CategoryHierarchyDb.startNodeIds

  /**
    * 挑选一个随机种子，后面会根据该种子节点，不断随机跳转得到一个子图。
    *
    * @return
    */
  private def seed(): Int = {
    val rand = Random.nextInt(startNodeIds.length)
    startNodeIds(rand)
  }

  /**
    * 类别cid的孩子和父亲构成的相邻节点
    *
    * @param cid
    * @return
    */
  private def neighborIds(cid: Int): IndexedSeq[(Int, Direction)] = {
    val children = CategoryHierarchyDb.getCNode(cid).toSeq.flatMap(_.childLinks)
    val parents = CategoryHierarchyDb.getParentIds(cid)

    if (parents.isEmpty)
    //如果父节点为空，说明到达了第一级节点，此时可以跳转到其他一级节点
      (children.map((_, Down)) ++ startNodeIds.filter(_ != cid).map((_, Down))).toIndexedSeq
    else
      (children.map((_, Down)) ++ parents.map((_, Up))).toIndexedSeq
  }

  /**
    * 从节点fromId进行一次跳跃, 返回跳到的目标地址
    */
  private def jump(fromId: Int): (Int, Direction) = {
    val ids = neighborIds(fromId)
    val rand = Random.nextInt(ids.length)
    ids(rand)
  }

  def generate(size: Int): mutable.Set[(Int, Int)] =
    extractSubGraph(seed(), size)

  /**
    * 抽取子图，从节点fromId开始，随机游走，跳转到下一个节点上.
    * 不同跳转的数量达到size为止
    *
    * @param fromId
    */
  private def extractSubGraph(fromId: Int, size: Int): mutable.Set[(Int, Int)] = {
    //记录随机游走得到的跳转（父，子）对
    val pairs = mutable.Set.empty[(Int, Int)]

    var lastId = fromId
    while (pairs.size < size) {
      val dest = jump(lastId)
      //过滤掉没有跳转（父子相同）的情况
      if (dest._1 != lastId && !startNodeIds.contains(dest._1)) {
        //用二元组(parent, child)记录由父到子的行走路径
        val pair = if (dest._2 == Up) (dest._1, lastId) else (lastId, dest._1)
        pairs += pair
      }

      lastId = dest._1
    }

    postFixPath(pairs)
  }

  /**
    * 将不是从第一级类别开始的目录，补充上父路径
    */
  private def postFixPath(pairs: mutable.Set[(Int, Int)]): mutable.Set[(Int, Int)] = {
    val groups: Map[Int, Seq[(Int, Int)]] = pairs.toSeq.flatMap {
      case (from, to) =>
        //收集节点的入度
        Seq((from, 0), (to, 1))
    }.groupBy(_._1)

    //入度为0的节点集合
    val zeroDegreeIds: Set[Int] = groups.map {
      case (k, v) =>
        //变换为(节点id，入度)
        (k, v.map(_._2).sum)
    }.filter(_._2 == 0).keys.toSet

    val okIds = (groups.keys.toSet -- zeroDegreeIds).toArray
    val processIds: mutable.Set[Int] = mutable.Set(ArraySeq.unsafeWrapArray(okIds): _ *)

    zeroDegreeIds foreach {
      id =>
        var current = id
        var stop = false

        while (!startNodeIds.contains(current) && !processIds.contains(current)) {
          //如果没有父节点，则随机挑选一个父节点，加入集合中
          val parents = CategoryHierarchyDb.getParentIds(current)
          val idx = Random.nextInt(parents.length)
          val parentId = parents(idx)
          val pair = (parentId, current)
          pairs += pair
          processIds += current

          current = parentId
        }
    }

    pairs
  }

  /**
    * 将得到的由父子节点对构成的子图转换为dot语法，输出到dot文件中，方便可视化呈现
    * DOT语法参考Graphviz
    */
  def toDotFile(pairs: Seq[(Int, Int)], dotFile: String): File = {
    val ids: Seq[Int] = pairs.flatMap(pair => List(pair._1, pair._2))
    val names = ids.map {
      id =>
        CategoryDb.getNameById(id).getOrElse("<EMPTY>")
    }

    val tips: Seq[String] = ids.zip(names).map {
      case (id, name) =>
        s"""$id [label="$id", xlabel="$name"]"""
    }

    val nodeText = tips.mkString("\n")
    val edgeText = pairs.map {
      case (p, c) => s"$p -> $c"
    }.mkString("\n")

    val dotText =
      s"""
         |digraph g {
         |  fontname = "Microsoft Yahei"
         |  graph [ordering="out"];
         |  margin=0;
         |  $nodeText
         |  $edgeText
         |}
    """.stripMargin

    File(dotFile).writeText(dotText)
    // Runtime.getRuntime.exec("dot -Tpdf /tmp/tree.dot -o /tmp/tree.pdf")
    //Runtime.getRuntime.exec(s"dot -Tpng /tmp/tree-$i.dot -o /tmp/test-$i.png")
  }

  def main(args: Array[String]): Unit = {
    val cid = 693661
    print("parents:")
    println(CategoryHierarchyDb.getParentIds(35321013).mkString(", "))
    println(CategoryDb.getNameById(690747))

    neighborIds(cid).foreach {
      case (id, d) =>
        val name = CategoryDb.getNameById(id).getOrElse("<EMPTY>")
        println(s"$id \t $name ($d)")
    }

    for (i <- 1 to 10) {
      val pairs = generate(30)

      toDotFile(pairs.toSeq, s"/tmp/tree-$i.dot")
      Runtime.getRuntime.exec(s"dot -Tpng /tmp/tree-$i.dot -o /tmp/test-$i.png")
    }
  }
}
