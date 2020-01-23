package wiki.dig.store.db

import java.io._
import java.nio.charset.StandardCharsets

import com.google.common.collect.Lists
import org.rocksdb._
import org.slf4j.LoggerFactory
import wiki.dig.MyConf
import wiki.dig.store.db.ast.{Db, DbHelper}
import wiki.dig.store.repo._
import wiki.dig.util.ByteUtil

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.Source

/**
  * 把Page的信息保存到RocksDB数据库中，里面记录的信息包括：
  *
  * Page的id:name双向映射关系，来自于数据库的Page. 由于有别名的存在，一个ID会对应到一个
  * 标准的词条名称上，但是有多个名称（规范的和非规范的）映射到一个id上。
  *
  * 页面的入链，来自于page_inlinks
  *
  * 页面的出链，来自于page_outlinks
  *
  * 页面指向的类别，来自于page_categories
  *
  * Page Redirects，来自于表：page_redirects
  */
object PageDb extends Db with DbHelper {
  val LOG = LoggerFactory.getLogger(this.getClass)

  import StandardCharsets.UTF_8

  val dbPath = new File(MyConf.dbRootDir, "page/main")
  if (!dbPath.getParentFile.exists())
    dbPath.getParentFile.mkdirs()

  RocksDB.loadLibrary()

  val options = new DBOptions().setCreateIfMissing(!MyConf.pageDbReadOnly)
    .setMaxBackgroundCompactions(10)
    .setCreateMissingColumnFamilies(!MyConf.pageDbReadOnly)

  protected val cfNames = Lists.newArrayList[ColumnFamilyDescriptor](
    new ColumnFamilyDescriptor("default".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("name2id".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("disambiguation".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("inlinks".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("outlinks".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("category".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("redirects".getBytes(UTF_8))
  )

  protected val cfHandlers = Lists.newArrayList[ColumnFamilyHandle]

  protected val db = RocksDB.open(options, dbPath.getAbsolutePath, cfNames, cfHandlers)

  protected val id2nameHandler: ColumnFamilyHandle = cfHandlers.get(0)
  protected val name2idHandler: ColumnFamilyHandle = cfHandlers.get(1)
  protected val disambiHandler: ColumnFamilyHandle = cfHandlers.get(2)
  protected val inlinksHandler: ColumnFamilyHandle = cfHandlers.get(3)
  protected val outlinksHandler: ColumnFamilyHandle = cfHandlers.get(4)
  protected val categoryHandler: ColumnFamilyHandle = cfHandlers.get(5)
  protected val redirectsHandler: ColumnFamilyHandle = cfHandlers.get(6)

  def build(startId: Int = 0, batchSize: Int = 1000) = {
    //val maxId = 58046434 //最大的id
    val maxId = Await.result(PageRepo.maxId(), Duration.Inf).get

    var fromId = startId

    while (fromId < maxId) {
      println(s"process $fromId / $maxId ...")
      val pages = Await.result(PageRepo.listBasicInfoFromId(fromId, batchSize), Duration.Inf)
      pages.foreach {
        case (id, name, disambiguation) =>
          //print(".")
          saveIdName(id, name)
          saveInlinks(id)
          saveOutlinks(id)

          //只记录是消歧义的页面，其他情况默认为非歧义页面
          if (disambiguation) {
            saveDisambiguation(id)
          }

          //保存页面对应的分类
          saveCategories(id)

          //保存指向该页面的别名
          saveRedirects(id)

          fromId = id
      }
      //println()

      fromId = fromId + 1
    }

    println("DONE")
  }

  /**
    * 获取当前页面的入链和出链之和
    */
  def getLinkedCount(pageId: Int): Int = {
    getInlinkCount(pageId).getOrElse(0) + getOutlinkCount(pageId).getOrElse(0)
  }

  /**
    * 创建ID和名称的双向映射，方便根据ID查名称，或者根据名称查ID
    */
  private def saveIdName(id: Int, name: String): Unit = {
    val idBytes = ByteUtil.int2bytes(id)
    val nameBytes = ByteUtil.string2bytes(name)
    db.put(id2nameHandler, idBytes, nameBytes)

    //转换为小写形式
    val nameBytes2 = ByteUtil.string2bytes(name.toLowerCase)
    db.put(name2idHandler, nameBytes2, idBytes)
  }

  def getNameById(id: Int): Option[String] = Option(
    db.get(id2nameHandler, ByteUtil.int2bytes(id))
  ).map(ByteUtil.bytes2string)

  def getIdByName(name: String): Option[Int] = Option(
    db.get(name2idHandler, ByteUtil.string2bytes(name.toLowerCase()))
  ).map(ByteUtil.bytes2Int)


  /**
    * 保存文章的入链列表
    */
  private def saveInlinks(id: Int) = {
    val links = Await.result(PageInlinkRepo.findInlinksById(id), Duration.Inf)
    val key = ByteUtil.int2bytes(id)
    val value = getBytesFromIntSeq(links)
    db.put(inlinksHandler, key, value)
  }

  /**
    * 获取文章id的入链结果
    */
  def getInlinks(id: Int): Seq[Int] = Option(
    db.get(inlinksHandler, ByteUtil.int2bytes(id))
  ) match {
    case Some(bytes) => readIntSeqFromBytes(bytes)
    case None => Seq.empty
  }

  def getInlinkCount(id: Int): Option[Int] = Option(
    db.get(inlinksHandler, ByteUtil.int2bytes(id))
  ).map(readSeqSizeFromBytes)

  private def saveOutlinks(id: Int) = {
    val links = Await.result(PageOutlinkRepo.findOutlinksById(id), Duration.Inf)
    val key = ByteUtil.int2bytes(id)
    val value = getBytesFromIntSeq(links)
    db.put(outlinksHandler, key, value)
  }

  def getOutlinks(id: Int): Seq[Int] = Option(
    db.get(outlinksHandler, ByteUtil.int2bytes(id))
  ) match {
    case Some(bytes) => readIntSeqFromBytes(bytes)
    case None => Seq.empty
  }

  def getOutlinkCount(id: Int): Option[Int] = Option(
    db.get(outlinksHandler, ByteUtil.int2bytes(id))
  ).map(readSeqSizeFromBytes)

  private def saveCategories(id: Int) = {
    val ids = Await.result(PageCategoryRepo.findCategoriesById(id), Duration.Inf)
    val key = ByteUtil.int2bytes(id)
    val value = getBytesFromIntSeq(ids)
    db.put(categoryHandler, key, value)
  }

  def getCategories(id: Int): Seq[Int] = Option(
    db.get(categoryHandler, ByteUtil.int2bytes(id))
  ) match {
    case Some(bytes) => readIntSeqFromBytes(bytes)
    case None => Seq.empty
  }

  def getCategoryCount(id: Int): Option[Int] = Option(
    db.get(categoryHandler, ByteUtil.int2bytes(id))
  ).map(readSeqSizeFromBytes)

  private def saveRedirects(id: Int) = {
    val redirects = Await.result(PageRedirectRepo.findRedirects(id), Duration.Inf)
    val key = ByteUtil.int2bytes(id)
    val value = getBytesFromStringSeq(redirects)
    db.put(redirectsHandler, key, value)

    //同时把所有的redirects记录到name2idHandler
    redirects.foreach {
      r =>
        val nameBytes = ByteUtil.string2bytes(r.toLowerCase)
        db.put(name2idHandler, nameBytes, key)
    }
  }

  def getRedirects(id: Int): Seq[String] = Option(
    db.get(redirectsHandler, ByteUtil.int2bytes(id))
  ) match {
    case Some(bytes) => readStringSeqFromBytes(bytes)
    case None => Seq.empty
  }

  def getRedirectCount(id: Int): Option[Int] = Option(
    db.get(redirectsHandler, ByteUtil.int2bytes(id))
  ).map(readSeqSizeFromBytes)

  /**
    * 记录id为消歧义页面
    *
    * @param id
    */
  private def saveDisambiguation(id: Int): Unit = {
    val key = ByteUtil.int2bytes(id)
    val value: Array[Byte] = Array(1)
    db.put(disambiHandler, key, value)
  }

  /**
    * 判断id是否为消歧义页面
    *
    * @param id
    * @return
    */
  def isDisambiguation(id: Int): Boolean = {
    val key = ByteUtil.int2bytes(id)
    db.get(disambiHandler, key) != null
  }

  /**
    * 数据库名字
    */
  def dbName: String = "Page DB"

  override def close(): Unit = {
    print(s"==> Close $dbName ... ")
    cfHandlers.forEach(h => h.close())
    db.close()
    println("DONE.")
  }


  def main(args: Array[String]): Unit = {
    case class Config(inFile: Option[File] = None,
                      outFile: Option[File] = None,
                      show: Option[String] = None)

    val parser = new scopt.OptionParser[Config]("bin/page-content-db") {
      head("output page content from id text file.")

      opt[File]('i', "inFile").optional().action((x, c) =>
        c.copy(inFile = Some(x))).text("input file of page id lines")

      opt[File]('o', "outFile").optional().action((x, c) =>
        c.copy(outFile = Some(x))).text("output file of page id and content")

      opt[String]("show").optional().action((x, c) =>
        c.copy(show = Some(x))).text("show page content by id/name")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        if (config.inFile.nonEmpty && config.outFile.nonEmpty) {
          PageContentDb.open()
          val it = Source.fromFile(config.inFile.get).getLines()
          PageContentDb.output(it, config.outFile.get)
          PageContentDb.close()
          LOG.info("DONE.")
        } else if (config.show.nonEmpty) {
          val idOrName = config.show.get
          val (id: Int, name: String) = if (idOrName.toIntOption.nonEmpty) {
            (idOrName.toInt, getNameById(idOrName.toInt).getOrElse("<EMPTY>"))
          } else {
            (getIdByName(idOrName).getOrElse(0), idOrName)
          }
          val content = PageContentDb.getContent(id).getOrElse("<EMPTY>")
          println(s"$id\t\t\t$name")
          println("---------------")
          println(content)
        } else {
          println(parser.usage)
        }
      case None =>
        println("""Wrong parameters :(""".stripMargin)
    }
  }
}