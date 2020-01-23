package wiki.dig.expt

import java.io._
import java.nio.charset.StandardCharsets

import com.google.common.collect.Lists
import org.rocksdb._
import org.slf4j.LoggerFactory
import org.zhinang.util.GZipUtils
import wiki.dig.MyConf
import wiki.dig.parser.WikiPage
import wiki.dig.store.db.ast.{Db, DbHelper}
import wiki.dig.store.db.{CategoryHierarchyDb, PageContentDb, PageDb}
import wiki.dig.util.ByteUtil

/**
  * 把试验用的Page的信息保存到RocksDB数据库中，里面记录的信息包括：
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
  *
  * 页面去除维基标记后的文本内容
  */
object ExptPageDb extends Db with DbHelper {
  val LOG = LoggerFactory.getLogger(this.getClass)

  import StandardCharsets.UTF_8

  val dbPath = new File(MyConf.dbRootDir, "expt/page")
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
    new ColumnFamilyDescriptor("redirects".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("content".getBytes(UTF_8))
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
  protected val contentHandler: ColumnFamilyHandle = cfHandlers.get(7)

  def build(startId: Int = 9972945, batchSize: Int = 1000) = {
    //将层次类别中涉及的文章id输出到文件中
    val pageIdFile = new File("./expt/page_id.txt")
    val pageIds = CategoryHierarchyDb.dumpPageIds(pageIdFile)
    var cnt = 0
    pageIds foreach {
      case pageId =>
        cnt += 1
        val title = PageDb.getNameById(pageId)
        val plainText = PageContentDb.getContent(pageId).flatMap {
          t =>
            WikiPage.getPlainText(t)
        }

        if (title.nonEmpty && plainText.nonEmpty) {
          val inlinks = PageDb.getInlinks(pageId) filter pageIds.contains
          val outlinks = PageDb.getOutlinks(pageId) filter pageIds.contains
          val redirects = PageDb.getRedirects(pageId)
          val categoryIds = PageDb.getCategories(pageId)
          saveIdName(pageId, title.get)
          saveInlinks(pageId, inlinks)
          saveOutlinks(pageId, outlinks)
          saveRedirects(pageId, redirects)
          saveCategories(pageId, categoryIds)
          saveContent(pageId, plainText.get)

          if (PageDb.isDisambiguation(pageId)) saveDisambiguation(pageId)
        }

        if (cnt % 1000 == 0) {
          println(s"process $cnt / ${pageIds.size} ...")
        }
    }

    println("DONE")
  }

  /**
    * 获取当前页面的入链和出链之和
    */
  def getLinkedCount(cid: Int): Int = {
    getInlinkCount(cid).getOrElse(0) + getOutlinkCount(cid).getOrElse(0)
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


  private def saveContent(id: Int, content: String) = {
    val key = ByteUtil.int2bytes(id)
    val value = GZipUtils.compress(content.getBytes(UTF_8))
    //    val value = content.getBytes(UTF_8)
    db.put(contentHandler, key, value)
  }

  def getContent(id: Int): Option[String] = Option(
    db.get(contentHandler, ByteUtil.int2bytes(id))
  ) match {
    case Some(bytes) =>
      val unzipped = GZipUtils.decompress(bytes)
      Option(new String(unzipped, UTF_8))

    case None => None
  }

  /**
    * 保存文章的入链列表
    */
  private def saveInlinks(id: Int, links: Seq[Int]) = {
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

  private def saveOutlinks(id: Int, links: Seq[Int]) = {
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

  private def saveCategories(id: Int, categoryIds: Seq[Int]) = {
    val key = ByteUtil.int2bytes(id)
    val value = getBytesFromIntSeq(categoryIds)
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

  private def saveRedirects(id: Int, redirects: Seq[String]) = {
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
  def dbName: String = "Expt Page DB"

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

    val parser = new scopt.OptionParser[Config]("bin/expt-content-db") {
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

      case None =>
        println( """Wrong parameters :(""".stripMargin)
    }
  }
}