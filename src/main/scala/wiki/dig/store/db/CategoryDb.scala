package wiki.dig.store.db

import java.io._
import java.nio.charset.StandardCharsets

import com.google.common.collect.Lists
import org.rocksdb._
import org.slf4j.LoggerFactory
import org.zhinang.util.StringUtils
import wiki.dig.MyConf
import wiki.dig.store.db.ast.{Db, DbHelper}
import wiki.dig.store.repo.{CategoryInlinkRepo, CategoryOutlinkRepo, CategoryPageRepo, CategoryRepo}
import wiki.dig.util.ByteUtil

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * 把Category的信息保存到RocksDB数据库中，里面记录的信息包括：
  *
  * 类别的id:name双向映射关系，来自于数据库的Categoryb.
  *
  * 类别的入链，来自于category_inlinks
  *
  * 类别的出链，来自于category_outlinks
  *
  * 类别指向的页面，来自于category_pages.
  */
object CategoryDb extends Db with DbHelper {
  val LOG = LoggerFactory.getLogger(this.getClass)

  import StandardCharsets.UTF_8

  val dbPath = new File(MyConf.dbRootDir, "category/main")
  if (!dbPath.getParentFile.exists())
    dbPath.getParentFile.mkdirs()


  RocksDB.loadLibrary()

  val options = new DBOptions().setCreateIfMissing(!MyConf.categoryDbReadOnly)
    .setMaxBackgroundCompactions(10)
    .setCreateMissingColumnFamilies(!MyConf.categoryDbReadOnly)

  protected val cfNames = Lists.newArrayList[ColumnFamilyDescriptor](
    new ColumnFamilyDescriptor("default".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("name2id".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("inlinks".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("outlinks".getBytes(UTF_8)),
    new ColumnFamilyDescriptor("pages".getBytes(UTF_8))
  )

  protected val cfHandlers = Lists.newArrayList[ColumnFamilyHandle]

  protected val db = RocksDB.open(options, dbPath.getAbsolutePath, cfNames, cfHandlers)

  protected val id2nameHandler: ColumnFamilyHandle = cfHandlers.get(0)
  protected val name2idHandler: ColumnFamilyHandle = cfHandlers.get(1)
  protected val inlinksHandler: ColumnFamilyHandle = cfHandlers.get(2)
  protected val outlinksHandler: ColumnFamilyHandle = cfHandlers.get(3)
  protected val pagesHandler: ColumnFamilyHandle = cfHandlers.get(4)

  def build() = {
    val pageSize = 5000
    val count = Await.result(CategoryRepo.count(), Duration.Inf)
    val pages = count / pageSize + 1
    (1 to pages) foreach {
      page =>
        println(s"process $page / $pages ...")
        val categories = Await.result(CategoryRepo.list(page, pageSize), Duration.Inf)

        categories.foreach {
          c =>
            saveIdName(c.id, c.name)
            saveInlinks(c.id)
            saveOutlinks(c.id)
            savePages(c.id)
        }
    }

    println("DONE")
  }

  /**
    * 获取当前分类的入链，出链，和页面数量之和
    */
  def getLinkedCount(cid: Int): Int = {
    getInlinkCount(cid).getOrElse(0) +
      getOutlinkCount(cid).getOrElse(0) +
      getPageCount(cid).getOrElse(0)
  }

  /**
    * 创建类别的ID和名称的双向映射，方便根据ID查名称，或者根据名称查ID
    */
  private def saveIdName(id: Int, name: String): Unit = {
    val idBytes = ByteUtil.int2bytes(id)
    val nameBytes = ByteUtil.string2bytes(name)
    db.put(id2nameHandler, idBytes, nameBytes)
    db.put(name2idHandler, nameBytes, idBytes)
  }

  def getNameById(id: Int): Option[String] = Option(
    db.get(id2nameHandler, ByteUtil.int2bytes(id))
  ).map(ByteUtil.bytes2string)

  def getIdByName(name: String): Option[Int] = Option(
    db.get(name2idHandler, ByteUtil.string2bytes(name))
  ).map(ByteUtil.bytes2Int)

  private def saveInlinks(cid: Int) = {
    val links = Await.result(CategoryInlinkRepo.findInlinksById(cid), Duration.Inf)
    val key = ByteUtil.int2bytes(cid)
    val value = getBytesFromIntSeq(links)
    db.put(inlinksHandler, key, value)
  }

  def getInlinks(cid: Int): Seq[Int] = Option(
    db.get(inlinksHandler, ByteUtil.int2bytes(cid))
  ) match {
    case Some(bytes) => readIntSeqFromBytes(bytes)
    case None => Seq.empty
  }

  def getInlinkCount(cid: Int): Option[Int] = Option(
    db.get(inlinksHandler, ByteUtil.int2bytes(cid))
  ).map(readSeqSizeFromBytes)

  private def saveOutlinks(cid: Int) = {
    val links = Await.result(CategoryOutlinkRepo.findOutlinksById(cid), Duration.Inf)
    val key = ByteUtil.int2bytes(cid)
    val value = getBytesFromIntSeq(links)
    db.put(outlinksHandler, key, value)
  }

  def getOutlinks(cid: Int): Seq[Int] = Option(
    db.get(outlinksHandler, ByteUtil.int2bytes(cid))
  ) match {
    case Some(bytes) => readIntSeqFromBytes(bytes)
    case None => Seq.empty
  }

  def getOutlinkCount(cid: Int): Option[Int] = Option(
    db.get(outlinksHandler, ByteUtil.int2bytes(cid))
  ).map(readSeqSizeFromBytes)

  private def savePages(cid: Int) = {
    val pageIds = Await.result(CategoryPageRepo.findPagesById(cid), Duration.Inf)
    val key = ByteUtil.int2bytes(cid)
    val value = getBytesFromIntSeq(pageIds)
    db.put(pagesHandler, key, value)
  }

  def getPages(cid: Int): Seq[Int] = Option(
    db.get(pagesHandler, ByteUtil.int2bytes(cid))
  ) match {
    case Some(bytes) => readIntSeqFromBytes(bytes)
    case None => Seq.empty
  }

  def getPageCount(cid: Int): Option[Int] = Option(
    db.get(pagesHandler, ByteUtil.int2bytes(cid))
  ).map(readSeqSizeFromBytes)

  /**
    * 数据库名字
    */
  def dbName: String = "Category DB"

  override def close(): Unit = {
    print(s"==> Close $dbName ... ")
    cfHandlers.forEach(h => h.close())
    db.close()
    println("DONE.")
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Usage: category-db id or name")
    } else {
      if (StringUtils.isNumber(args(0))) {
        val id = args(0).toInt
        val name = getNameById(id)
        val pages = getPages(id)
        println(s"$id\t${name.getOrElse("<empty>")}")
        println(s"\t${pages.mkString(" ")}")
      } else {
        val name = args(0).trim
        val id = getIdByName(name)
        val pages = if (id.isEmpty) Seq.empty[Int] else getPages(id.get)
        println(s"${id.map(_.toString).getOrElse("<empty>")}\t$name")
        println(s"\t${pages.mkString(" ")}")
      }
      CategoryDb.close()
    }
  }
}