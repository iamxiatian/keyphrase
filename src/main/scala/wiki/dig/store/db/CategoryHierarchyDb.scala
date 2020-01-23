package wiki.dig.store.db

import java.io._
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern

import com.google.common.collect.Lists
import com.google.common.io.Files
import org.rocksdb._
import org.slf4j.LoggerFactory
import wiki.dig.MyConf
import wiki.dig.store.db.ast.{Db, DbHelper}
import wiki.dig.store.repo.CategoryRepo
import wiki.dig.util.ByteUtil

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

/**
  * wiki层级体系数据库，记录了层级之间的父子关系. 层级体系数据库依赖于CategoryDb主数据库
  * 事先构建成功。(see CategoryDb.build)
  *
  * 默认列族(default): 记录每个节点及其子节点信息；
  *
  * meta列族：记录元数据，包括：每一层拥有的文章总数量
  *
  * articleCount列族：记录每一个节点拥有的文章数量。
  * 第一次构建完成父子层级关系后，需要进一步计算每一个节点包含的文章数量，该数值包含该
  * 节点的子节点的文章数量，合并计算作为当前节点的文章数量。构建时，从最后一个层级的节点
  * 开始，逐层向上计算，计算结果保存在了"articleCount"列族.
  *
  * parentCount列族：记录每一个节点拥有的父节点数量
  */
object CategoryHierarchyDb extends Db with DbHelper {
  val LOG = LoggerFactory.getLogger(this.getClass)

  import StandardCharsets.UTF_8

  val dbPath = new File(MyConf.dbRootDir, "category/hierarchy")
  if (!dbPath.getParentFile.exists())
    dbPath.getParentFile.mkdirs()

  RocksDB.loadLibrary()

  val options = new DBOptions().setCreateIfMissing(true)
    .setMaxBackgroundCompactions(10)
    .setCreateMissingColumnFamilies(true)

  protected val cfNames = Lists.newArrayList[ColumnFamilyDescriptor](
    new ColumnFamilyDescriptor("default".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("meta".getBytes(UTF_8)), //元数据族
    new ColumnFamilyDescriptor("articleCount".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("parentCount".getBytes(UTF_8))
  )

  protected val cfHandlers = Lists.newArrayList[ColumnFamilyHandle]

  protected val db = RocksDB.open(options, dbPath.getAbsolutePath, cfNames, cfHandlers)

  protected val defaultHandler: ColumnFamilyHandle = cfHandlers.get(0)
  protected val metaHandler: ColumnFamilyHandle = cfHandlers.get(1)
  protected val articleCountHandler: ColumnFamilyHandle = cfHandlers.get(2)
  protected val parentCountHandler: ColumnFamilyHandle = cfHandlers.get(3)

  val Max_Depth = 5


  case class AcTuple(var directCount: Int, var recursiveCount: Long = 0) {
    def toBytes: Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val dos = new DataOutputStream(out)
      dos.writeInt(directCount)
      dos.writeLong(recursiveCount)
      dos.close()
      out.close()
      out.toByteArray
    }

    def +(o: AcTuple) = AcTuple(directCount + o.directCount,
      recursiveCount + o.recursiveCount)
  }

  object AcTuple {
    def apply(bytes: Array[Byte]): AcTuple = {
      val din = new DataInputStream(new ByteArrayInputStream(bytes))
      val directCount = din.readInt()
      val recursiveCount = din.readLong()
      din.close()
      AcTuple(directCount, recursiveCount)
    }

    def empty = AcTuple(0, 0L)
  }

  def saveArticleCount(id: Int, count: AcTuple): Unit = {
    db.put(articleCountHandler, ByteUtil.int2bytes(id), count.toBytes)
  }

  def getArticleCount(id: Int): Option[AcTuple] = Option(
    db.get(articleCountHandler, ByteUtil.int2bytes(id))
  ).map {
    bytes =>
      AcTuple(bytes)
  }

  /**
    * 获取深度为depth的所有节点拥有的文章总数量
    *
    * @param depth
    * @return
    */
  def articleCountAtDepth(depth: Int): Option[AcTuple] = Option(
    db.get(metaHandler, s"depth:${depth}:articles".getBytes(UTF_8))
  ).map(AcTuple(_))

  /**
    * 获取一个节点所有的父节点
    *
    * @param cid
    * @return
    */
  def getParentIds(cid: Int): Seq[Int] = Option(
    db.get(parentCountHandler, ByteUtil.int2bytes(cid))
  ) match {
    case Some(value) => readIntSeqFromBytes(value)
    case None => Seq.empty[Int]
  }

  /**
    * 计算各个节点包含的文章数量，其子节点包含的文章数量也合并计算到该节点之中。    *
    * 因此，需要从底层向上计算。该方法依赖于build()函数事先执行完毕，构建出层次
    * 关系，才可以二次运行。
    */
  def calculateArticleCount(): Unit = {
    val ids = mutable.ListBuffer.empty[Int] //存放所有的id，深度依次递增

    //存放分类id拥有的文章数量（包含子类）、不包含子类的文章数量
    val articleCountCache = mutable.Map.empty[Int, AcTuple]

    //存放所有的id到父节点的映射
    val parentCache = mutable.Map.empty[Int, mutable.Set[Int]]

    //存放每层拥有的文章数量
    val depthCountCache = mutable.Map.empty[Int, AcTuple]

    val queue = mutable.Queue.empty[(Int, Int)]
    startNodeIds.foreach {
      id =>
        queue.enqueue((id, 1))
        parentCache.put(id, mutable.Set.empty[Int])
    }

    var counter = 0

    while (queue.nonEmpty) {
      val (cid, depth) = queue.dequeue()

      val key = ByteUtil.int2bytes(cid)

      getCNode(cid) match {
        case Some(node) =>
          counter += 1
          if (counter % 1000 == 0) {
            println(s"processing $counter, queue size: ${queue.size}")
          }

          //无重复的记录出现的ID
          if (!articleCountCache.contains(cid)) {
            val count = CategoryDb.getPageCount(cid).getOrElse(0)
            articleCountCache.put(cid, AcTuple(count, 0))
            ids.append(cid)

            if (depth <= Max_Depth) {
              node.childLinks.foreach {
                id =>
                  queue.enqueue((id, depth + 1))
                  //记录父节点
                  if (parentCache.contains(id)) {
                    parentCache(id) += cid
                  } else {
                    parentCache.put(id, mutable.Set(cid))
                  }
              }
            }
          }
        case None =>
          print("Error")
      }
    }

    //把各个类别以及对应的文章数量，记录到文本文件中
    //    val writer = Files.newWriter(new File("./cat.article.count.txt"), UTF_8)
    //    writer.write("category\tarticle_count\n")
    //    countCache foreach {
    //      case( cid, count) =>
    //        writer.write(s"$cid\t$count\n")
    //    }
    //    writer.close()

    println("Iterate")
    counter = 0
    ids.reverse.foreach {
      cid =>
        counter += 1
        if (counter % 1000 == 0) {
          println(s"processing $counter / ${ids.size}")
        }
        getCNode(cid) match {
          case Some(node) =>
            val childCount = node.childLinks.map {
              child =>
                //子节点的文章数量，需要平分到父节点上
                val c: Long = articleCountCache.get(child)
                  .map(_.recursiveCount)
                  .getOrElse(0)
                val parentCount: Int = parentCache.get(child)
                  .map(_.size)
                  .getOrElse(1)

                c / parentCount
            }.sum
            //更新当前类别的数量，并记录到数据库
            val count = articleCountCache.get(cid)
              .map(_.directCount)
              .getOrElse(0)

            val tuple = AcTuple(count, count + childCount)
            articleCountCache.put(cid, tuple)
            saveArticleCount(cid, tuple)

            //记录深度
            depthCountCache.put(node.depth,
              depthCountCache.getOrElse(node.depth, AcTuple.empty) + tuple)

          case None =>
        }
    }

    parentCache.foreach {
      case (cid, parents) =>
        db.put(parentCountHandler,
          ByteUtil.int2bytes(cid),
          getBytesFromIntSeq(parents)
        )
    }

    depthCountCache foreach {
      case (depth, count) =>
        println(s"articles in depth $depth:\t $count")
        db.put(metaHandler,
          s"depth:${depth}:articles".getBytes(UTF_8),
          count.toBytes)
    }

    println(s"DONE, processed ${ids.size}")
  }

  def accept(name: String): Boolean = {
    val title = name.replaceAll("_", " ").toLowerCase()

    if (title.length > 7) { //保留1980s此类词条
      val startString = title.substring(0, 4)
      if (startString.toIntOption.nonEmpty) return false
    }

    //step 2: remove "list of xxxx" and "index of xxx"
    if (title.indexOf("index of ") >= 0 ||
      title.indexOf("list of") >= 0 ||
      title.indexOf("lists of") >= 0 || //新增加
      title.indexOf("(disambiguation)") >= 0) return false

    //以年份结尾的词条，符合年份时代结尾的形式文章，如``China national football team results (2000–09)''，因为这类文章的作用更类似于类别，起到信息组织的作用。
    val pattern = Pattern.compile("\\(\\d{4}(–|\\-)\\d{2,4}\\)$")
    if (pattern.matcher(title).find) return false

    return true
  }

  def build() = {
    val startNodes = Await.result(CategoryRepo.levelOne(), Duration.Inf).map(_.id)
    val queue = mutable.Queue.empty[(Int, Int)]

    var totalWeight: Long = 0L

    var counter = 0

    startNodes.foreach(id => queue.enqueue((id, 1)))

    startNodes.foreach(println)

    while (queue.nonEmpty) {
      val (cid, depth) = queue.dequeue()

      val key = ByteUtil.int2bytes(cid)

      if (getCNode(cid).isEmpty) {
        counter += 1
        if (counter % 1000 == 0) {
          println(s"processing $counter, queue size: ${queue.size}")
        }
        //之前没有保存过，已保证保存的depth最小。
        val outlinks = CategoryDb.getOutlinks(cid).filter {
          id =>
            CategoryDb.getNameById(id) match {
              case Some(name) =>
                accept(name)
              case None =>
                println(s"no name error: $id")
                false
            }
        }
        val weights = outlinks.map(CategoryDb.getLinkedCount(_))

        val weight = CategoryDb.getLinkedCount(cid)
        totalWeight += weight

        val node = CNode(depth, weight, outlinks, weights)
        db.put(key, node.toBytes())
        if (depth <= Max_Depth) {
          outlinks.foreach(id => queue.enqueue((id, depth + 1)))
        }
      } else {
        //println(s"$cid / $depth")
        print(".")
      }
    }

    db.put(metaHandler, "TotalWeight".getBytes(UTF_8), ByteUtil.long2bytes(totalWeight))
    println("DONE")
  }

  def getCNode(cid: Int): Option[CNode] = Option(
    db.get(ByteUtil.int2bytes(cid))
  ) map readCNode

  def getTotalWeight(): Long = Option(
    db.get(metaHandler, "TotalWeight".getBytes(UTF_8))
  ).map(ByteUtil.bytes2Long(_)).getOrElse(1)

  /**
    * 数据库名字
    */
  def dbName: String = "Category Hierarchy DB"

  override def close(): Unit = {
    print(s"==> Close Category Hierarchy Db ... ")
    cfHandlers.forEach(h => h.close())
    db.close()
    println("DONE.")
  }

  def readCNode(bytes: Array[Byte]): CNode = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))
    val depth = din.readInt()
    val weight = din.readInt()
    val count = din.readInt()
    val outlinks = (0 until count).map(_ => din.readInt())
    val weights = (0 until count).map(_ => din.readInt())
    din.close
    CNode(depth, weight, outlinks, weights)
  }

  /**
    * 入口类别的ID，位于“Main topic classification"下，入口类别有：
    * {{{
    * +----------|----------|------------------------+
    * | id | pageId | name |
    * +----------|----------|------------------------+
    * |   690747 |   690747 | Mathematics            |
    * |   691008 |   691008 | People                 |
    * |   691810 |   691810 | Philosophy             |
    * |   691928 |   691928 | Law                    |
    * |   692694 |   692694 | Religion               |
    * |   693555 |   693555 | History                |
    * |   693708 |   693708 | Sports                 |
    * |   693800 |   693800 | Geography              |
    * |   694861 |   694861 | Culture                |
    * |   695027 |   695027 | Politics               |
    * |   696603 |   696603 | Nature                 |
    * |   696763 |   696763 | Education              |
    * |   722196 |   722196 | Reference              |
    * |   751381 |   751381 | Health                 |
    * |  1004110 |  1004110 | Humanities             |
    * |  1633936 |  1633936 | Society                |
    * |  2389032 |  2389032 | Life                   |
    * |  2766046 |  2766046 | Events                 |
    * |  3260154 |  3260154 | World                  |
    * |  4892515 |  4892515 | Arts                   |
    * |  8017451 |  8017451 | Language               |
    * | 47642171 | 47642171 | Science_and_technology |
    * | 48005914 | 48005914 | Universe               |
    * +----------|----------|------------------------+
    * }}}
    *
    */
  val startNodeIds: Seq[Int] = {
    //如果从MySQL数据库中读取，则用底下注释的语句
    //Await.result(CategoryRepo.levelOne(), Duration.Inf).map(_.id)

    //为了避免连接MySQL，直接把ID写到如下的列表中
    if (MyConf.wikiLang == "zh")
      List(33576, 33578, 33580, 33582, 33586, 41219, 41504, 42326, 44066,
        44741, 58048, 59765, 65708, 75848, 258453, 326427, 326428, 414585,
        545519, 766030, 770007, 1041667)
    else
      List(690747, 691008, 691810, 691928, 692694, 693555, 693708, 693800,
        694861, 695027, 696603, 696763, 722196, 751381, 1004110, 1633936,
        2389032, 2766046, 3260154, 4892515, 8017451, 47642171, 48005914)

  }

  /**
    * 按照如下方式把层级信息输出到文本文件中：
    *
    * brief = true:
    * cate_id  cate_name
    *
    * brief = false:
    * cate_id  cate_name cate_level cate1_id cate2_id cate3_id (child ...)
    *
    * @param f
    */
  def output(f: File, brief: Boolean = false) = {
    val writer = Files.newWriter(f, UTF_8)
    val queue = mutable.Queue.empty[(Int, Int)]
    startNodeIds.foreach(id => queue.enqueue((id, 1)))

    var counter = 0
    while (queue.nonEmpty) {
      val (cid, depth) = queue.dequeue()

      val key = ByteUtil.int2bytes(cid)

      getCNode(cid) match {
        case Some(node) =>
          counter += 1
          if (counter % 1000 == 0) {
            println(s"processing $counter, queue size: ${queue.size}")
            writer.flush()
          }
          val name = CategoryDb.getNameById(cid).getOrElse("")
          writer.write(s"$cid\t${name}")
          if (brief == false) {
            writer.write(s"\t${node.depth}\t")
            writer.write(node.outlinks.mkString(", "))
          }
          writer.write("\n")

          //继续后续处理
          if (depth <= Max_Depth) {
            node.outlinks.foreach(id => queue.enqueue((id, depth + 1)))
          }
        case None =>
          //Error
          print("X")
      }
    }

    writer.close()
    println(s"\nDONE to file ${f.getCanonicalPath}")
  }

  /**
    * 按照如下方式把每个类别关联的文章ID集合输出到文本文件中：
    * cate_id, page1 id, page2 id, ...
    *
    * 同时，把所有出现的文章id，以无重复的方式记录到pageIdsFile
    *
    */
  def outputPageIds(cateogryPagesFile: File = new File("./category_pages.txt"),
                    pageIdsFile: File = new File("./page_all_ids.txt")) = {
    println("Start to output page ids of each category")
    val writer = Files.newWriter(cateogryPagesFile, UTF_8)

    val pageIdSet = mutable.Set.empty[Int] //存放所有关联的网页ID
    val queue = mutable.Queue.empty[(Int, Int)]
    startNodeIds.foreach(cid => queue.enqueue((cid, 1)))

    var counter = 0
    while (queue.nonEmpty) {
      val (cid, depth) = queue.dequeue()

      val key = ByteUtil.int2bytes(cid)

      getCNode(cid) match {
        case Some(node) =>
          counter += 1
          if (counter % 1000 == 0) {
            println(s"processing $counter, queue size: ${queue.size}")
          }

          val pageIds = CategoryDb.getPages(cid)
          pageIds.foreach { pid => pageIdSet += pid }

          writer.write(s"$cid, ")
          writer.write(pageIds.mkString(", "))
          writer.write("\n")

          if (depth <= Max_Depth) {
            node.outlinks.foreach(id => queue.enqueue((id, depth + 1)))
          }
        case None =>
          //Error
          print("X")
      }
    }

    writer.close()
    println(s"DONE to file ${cateogryPagesFile.getCanonicalPath}")

    val pageIdsWriter = Files.newWriter(pageIdsFile, UTF_8)
    pageIdSet.foreach {
      id =>
        pageIdsWriter.write(id)
        pageIdsWriter.write("\n")
    }
    println("DONE!")
  }


  /**
    * 把所有出现的文章id，以无重复的方式记录到集合对象中
    */
  def dumpPageIds(pageIdFile: File = new File("./dump/page_id.txt")): Set[Int] = {
    println("Start to get page ids from hierarchy category")

    val pageIdSet = mutable.Set.empty[Int] //存放所有关联的网页ID
    val queue = mutable.Queue.empty[(Int, Int)]
    startNodeIds.foreach(cid => queue.enqueue((cid, 1)))

    var counter = 0
    while (queue.nonEmpty) {
      val (cid, depth) = queue.dequeue()

      val key = ByteUtil.int2bytes(cid)

      getCNode(cid) match {
        case Some(node) =>
          counter += 1
          if (counter % 1000 == 0) {
            println(s"processing $counter, queue size: ${queue.size}")
          }

          val pageIds = CategoryDb.getPages(cid)
          pageIds.foreach { pid => pageIdSet += pid }

          if (depth <= Max_Depth) {
            node.outlinks.foreach(id => queue.enqueue((id, depth + 1)))
          }
        case None =>
          //Error
          print("X")
      }
    }

    pageIdFile.getParentFile.mkdirs()

    val pageIdsWriter = Files.newWriter(pageIdFile, UTF_8)
    pageIdSet.foreach {
      id =>
        pageIdsWriter.write(id)
        pageIdsWriter.write("\n")
    }
    pageIdsWriter.close()
    println("DONE!")

    pageIdSet.toSet
  }

  /**
    * 抽样n个三角形, 即一个父类R和两个子类A,B，形成一个三角形(R, A, B).
    *
    * 输出格式为一个文本文件, 文件中的每一行为R, A, B
    *
    * @param n           抽样结果的预计数量
    * @param f           抽样结果保存的文件
    * @param isNameLabel true表示输出的是类别的名称，否则为类别的ID
    */
  def sample(n: Int,
             triangleFile: File = new File("./sample.triangle.ids.txt"),
             isNameLabel: Boolean = false): Unit = {
    val totalWeight = getTotalWeight().toInt

    val writer = Files.newWriter(triangleFile, UTF_8)

    //在每个节点上按比例抽样。
    val queue = mutable.Queue.empty[(Int, Int)]

    var counter = 0

    startNodeIds.foreach(id => queue.enqueue((id, 1)))

    println(s"total weight: $totalWeight")

    //存放出现在三角形中的所有类别的ID
    val categoryIdSet = mutable.Set.empty[Int]

    while (queue.nonEmpty) {
      val (cid, depth) = queue.dequeue()

      val key = ByteUtil.int2bytes(cid)

      getCNode(cid) match {
        case Some(node) =>
          counter += 1
          if (counter % 1000 == 0) {
            println(s"processing $counter, queue size: ${queue.size}")
          }
          val outlinks = node.outlinks

          if (outlinks.size >= 2) {
            //每个节点上都循环n次，但只有w*(max_depth - depth + 1)/totalWeights 的机率会被选中
            (0 until n) foreach {
              _ =>
                //抽样
                val r = Random.nextInt(totalWeight)
                if (r < node.weight) {
                  //被抽中了，再来抽子节点


                  val weights = node.weights
                  val childTotalWeights = weights.sum + 1

                  //挑选第一个子类
                  val x = pick(outlinks, weights, Random.nextInt(childTotalWeights))

                  def next(exceptValue: Int): Int = {
                    val v = pick(outlinks, weights, Random.nextInt(childTotalWeights))
                    if (v != exceptValue) v else next(exceptValue)
                  }

                  //挑选第2个子类，但不能和第一个重复
                  val y = next(x)

                  categoryIdSet += cid
                  categoryIdSet += x
                  categoryIdSet += y

                  //output (cid, x, y)
                  val line = if (isNameLabel)
                    s"${CategoryDb.getNameById(cid).getOrElse("")}, ${CategoryDb.getNameById(x).getOrElse("")}, ${CategoryDb.getNameById(y).getOrElse("")}\n"
                  else
                    s"$cid, $x, $y\n"

                  writer.write(line)
                  if (counter % 100 == 0) writer.flush()
                }
            }
          }

          //继续后续抽样处理
          if (depth <= Max_Depth) {
            outlinks.foreach(id => queue.enqueue((id, depth + 1)))
          }
        case None =>
          //Error
          print("X")
      }
    }
    writer.close()

    //把所有抽样结果中出现的类别id保存到文本文件中
    print("write category ids appeared in sample: ... ")
    val categoryIdWriter = Files.newWriter(new File("sample.category.ids.txt"), UTF_8)
    categoryIdSet.foreach {
      id =>
        categoryIdWriter.write(s"$id\n")
    }
    println("DONE!")

    //把所有抽样结果中出现的文章id保存到文本文件中
    print("write page ids appeared in sample: ... ")

    val catPageMappingWriter = Files.newWriter(new File("sample.cat_page.txt"), UTF_8)
    catPageMappingWriter.write("# category_id page_id1 page_id2 ...\n\n")
    val pageIdSet = mutable.Set.empty[Int]
    categoryIdSet.foreach {
      cid =>
        val pids = CategoryDb.getPages(cid)
        catPageMappingWriter.write(s"$cid ${pids.mkString(" ")}\n")

        pids.foreach {
          pid =>
            pageIdSet += pid
        }
    }

    val pageIdWriter = Files.newWriter(new File("sample.page.ids.txt"), UTF_8)
    pageIdSet.foreach {
      id =>
        pageIdWriter.write(s"$id\n")
    }
    println("DONE!")
  }

  /**
    * 从outlink中按概率随机挑选一个id
    *
    * @param outlinks
    * @param weights
    * @return
    */
  def pick(outlinks: Seq[Int], weights: Seq[Int], randNumber: Int): Int = {
    var accumulator = 0
    outlinks.zip(weights).find {
      case (id, w) =>
        if (w + accumulator > randNumber)
          true
        else {
          accumulator += w
          false
        }
    }.map(_._1).getOrElse(outlinks.head)
  }

  /**
    * 统计输出类别的数据, 输出一个Map，主键为分类的层，值为该层上的节点数量和文章数量
    */
  def dataInfo(): Map[Int, (Int, Int)] = {
    val processedIds = mutable.Set.empty[Int]

    val queue = mutable.Queue.empty[(Int, Int)]
    startNodeIds.foreach(id => queue.enqueue((id, 1)))

    // 每一层上的节点数量，拥有的文章数量
    val levelInfo = mutable.Map.empty[Int, (Int, Int)]

    var counter = 0
    while (queue.nonEmpty) {
      val (cid, depth) = queue.dequeue()

      if (processedIds.contains(cid)) {
        print(".") //skip processed id
      } else {
        getCNode(cid) match {
          case Some(node) =>
            counter += 1
            if (counter % 1000 == 0) {
              println(s"processing $counter, queue size: ${queue.size}")
            }

            val pair = levelInfo.getOrElse(node.depth, (0, 0))
            val articles = CategoryDb.getPageCount(cid).getOrElse(0)
            levelInfo(node.depth) = (pair._1 + 1, pair._2 + articles)

            //继续后续处理
            if (depth <= Max_Depth) {
              node.outlinks.foreach(id => queue.enqueue((id, depth + 1)))
            }
          case None => println("X")
        }

        processedIds += cid
      }
    }

    levelInfo.toMap
  }


  def main2(args: Array[String]): Unit = {
    // output(new File("./all.category.names.txt"), true)

    //println("Prepare to calculate article count...")
    //StdIn.readLine()


  }

  def main(args: Array[String]): Unit = {
    case class Config(allNodesToFile: Option[File] = None,
                      verbose: Boolean = true,
                      showStats: Boolean = false)

    val parser = new scopt.OptionParser[Config]("bin/category-hierarchy-db") {
      head("category-hierarchy-db.")

      opt[File]("allNodesToFile").optional()
        .action((x, c) => c.copy(allNodesToFile = Some(x)))
        .text("输出所有的节点到该文件中")

      opt[Boolean]("verbose").optional()
        .action((x, c) => c.copy(verbose = x))
        .text("输出节点名称时同时输出子节点")

      opt[Boolean]("showStats").optional()
        .action((x, c) => c.copy(showStats = x))
        .text("显示统计信息")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        if (config.allNodesToFile.nonEmpty) {
          println(s"把所有节点输出到文件：${config.allNodesToFile.get}")
          output(config.allNodesToFile.get, !config.verbose)
          println("DONE.")
        }

        if (config.showStats) {
          println("显示类别统计信息: ")
          val data = dataInfo()

          val totalNodes = data.values.map(_._1).sum
          val totalArticles = data.values.map(_._2).sum

          data foreach {
            case (depth, (nodes, articles)) =>
              val nodePercent = (nodes * 100.0 / totalNodes).formatted("%.2f%%")
              val articlePercent = (articles * 100.0 / totalArticles).formatted("%.2f%%")

              println(s"Level: $depth, \t nodes: $nodes ($nodePercent),\t articles: $articles ($articlePercent)")
          }

          println(s"total nodes: $totalNodes, total articles: $totalArticles")
        }

      case None =>
        println("""Wrong parameters :(""".stripMargin)
    }
    CategoryHierarchyDb.close()
  }
}

/**
  * 类别节点
  *
  * @param depth    当前类别节点的深度
  * @param weight   当前类别的权重
  * @param outlinks 当前类别节点的出链，即下级子类
  * @param weights  对应子类的权重，当前为出入量数量+页面数量
  */
case class CNode(depth: Int,
                 weight: Int,
                 outlinks: Seq[Int],
                 weights: Seq[Int]
                ) {

  /**
    * 获取当前节点的子节点，即出链并且深度为当前深度+1的类别
    */
  def childLinks: Seq[Int] = outlinks.filter(child =>
    CategoryHierarchyDb.getCNode(child).exists(_.depth == depth + 1)
  )

  def toBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    dos.writeInt(depth)
    dos.writeInt(weight)

    dos.writeInt(outlinks.size)
    outlinks.foreach(dos.writeInt(_))
    weights.foreach(dos.writeInt(_))
    dos.close()
    out.close()

    out.toByteArray
  }
}