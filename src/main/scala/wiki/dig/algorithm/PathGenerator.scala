package wiki.dig.algorithm

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files
import wiki.dig.store.db.{CategoryDb, CategoryHierarchyDb}

import scala.util.Random

/**
  * 生成文件的路径，生成方法：从第一级节点开始，以RandomWalk的方式，随机挑选路径
  * 在每一级别上，以随机概率判断是否停止跳跃。
  *
  */
object PathGenerator {

  import StandardCharsets.UTF_8

  def randomWalk(N: Int): Unit = {
    val depthNumbers: Seq[Int] = (1 to 6).toSeq

    //在第几层停止跳转
    val directCountDist: Seq[Long] = depthNumbers.map {
      d =>
        CategoryHierarchyDb.articleCountAtDepth(d).get.directCount.toLong
    }

    val writer = Files.newWriter(new File("./path.txt"), UTF_8)

    depthNumbers.zip(directCountDist).foreach {
      case (d, c) =>
        println(s"articles on depth $d ==> $c")
    }

    var count = 0
    while (count < N) {
      //本次生成的路径长度
      val pathLength = pick(depthNumbers, directCountDist)

      val cids = generatePath(if (pathLength > 2) pathLength else 3)

      val articleIds = CategoryDb.getPages(cids.last)
      if (articleIds.nonEmpty) {
        count += 1
        val rand = Random.nextInt(articleIds.length)
        val pickedArticleId = articleIds(rand)
        writer.write(s"$pickedArticleId\t${cids.mkString("\t")}\n")
        if (count % 1000 == 0) {
          println(s"$count / $N ")
          writer.flush()
        }
      } else {
        //println(s"empty articles: path len: $pathLength, category: ${cids.last}")
      }
    }
    writer.close()
  }

  def generatePath(pathLength: Int): Seq[Int] = {

    /**
      * 从当前的节点里面，选择一个跳转，返回选中的节点，以及下一步的候选集合
      */
    def walk(nodeIds: Seq[Int]): (Int, Seq[Int]) = {
      if (nodeIds.isEmpty)
        (0, Seq.empty[Int])
      else {
        val weights: Seq[Long] = nodeIds.map {
          id =>
            CategoryHierarchyDb.getArticleCount(id).map(_.recursiveCount).getOrElse(0L)
        }
        val id = pick(nodeIds, weights)
        val childNodes = CategoryHierarchyDb.getCNode(id).get.childLinks
        (id, childNodes)
      }
    }

    var nodeIds = CategoryHierarchyDb.startNodeIds

    (1 to pathLength).map {
      _ =>
        val (c, children) = walk(nodeIds)
        nodeIds = children
        c
    }.toSeq
  }

  /**
    * 根据随机数，挑选一个落在权重范围里面的元素。例如：elements为[1,2,3,4,5]，
    * 权重为[100,3,2,10,9]， 生成的随机数为3，则落在第一个元素上，返回第一个元素1.
    */
  def pick(elements: Seq[Int], weights: Seq[Long]): Int = {
    val total = weights.sum
    if (total == 0) {
      elements(Random.nextInt(elements.size))
    } else {
      val randNumber = Random.nextDouble()
      val scores = weights.map(_ / (total.toDouble))
      var accumulator = 0.0

      elements.zip(scores).find {
        case (e, s) =>
          if (s + accumulator > randNumber)
            true
          else {
            accumulator += s
            false
          }
      }.map(_._1).getOrElse(elements.head)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Usage: path-generator numbers")
    } else {
      randomWalk(args(0).toInt)
    }
  }
}
