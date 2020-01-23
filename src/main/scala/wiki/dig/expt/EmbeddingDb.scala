package wiki.dig.expt

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.collect.Lists
import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyHandle, DBOptions, RocksDB}
import org.slf4j.LoggerFactory
import wiki.dig.MyConf
import wiki.dig.store.db.ast.{Db, DbHelper}

import scala.io.Source

/**
  * 嵌入表示的数据库，把GloVE的训练结果从文本文件转存到RocksDB中，方便随机访问
  *
  */
object EmbeddingDb extends Db with DbHelper {
  val LOG = LoggerFactory.getLogger(this.getClass)

  import StandardCharsets.UTF_8

  val dbPath = new File(MyConf.dbRootDir, "embedding")
  if (!dbPath.getParentFile.exists())
    dbPath.getParentFile.mkdirs()

  RocksDB.loadLibrary()

  val options = new DBOptions().setCreateIfMissing(true)
    .setMaxBackgroundCompactions(10)
    .setCreateMissingColumnFamilies(true)

  protected val cfNames = Lists.newArrayList[ColumnFamilyDescriptor](
    new ColumnFamilyDescriptor("default".getBytes(UTF_8)), //默认为GloVE 100
    new ColumnFamilyDescriptor("glove50".getBytes(UTF_8))
  )

  protected val cfHandlers = Lists.newArrayList[ColumnFamilyHandle]

  protected val db = RocksDB.open(options, dbPath.getAbsolutePath, cfNames, cfHandlers)

  protected val defaultHandler: ColumnFamilyHandle = cfHandlers.get(0)
  protected val glove50Handler: ColumnFamilyHandle = cfHandlers.get(1)

  def build(f: File) = {
    val source = Source.fromFile(f, "UTF-8")
    source.getLines().filter(_.nonEmpty).foreach {
      line =>
        //office-bearers 0.1654 0.11989 -0.53959 0.0072575...
        val items = line.split(" ")
        val name = items(0)
        val scores = items.tail.map(_.toFloat)
        db.put(defaultHandler, name.getBytes(UTF_8), getBytesFromFloatSeq(scores))
        print(".")
    }
    source.close()
    println("DONE")
  }

  def find(name: String): Option[Seq[Float]] =
    Option(db.get(defaultHandler, name.trim.toLowerCase().getBytes(UTF_8))).map{
      readFloatSeqFromBytes(_)
    }

  /**
    * 数据库名字
    */
  def dbName: String = "Embedding DB"

  override def close(): Unit = {
    print(s"==> Close $dbName ... ")
    cfHandlers.forEach(h => h.close())
    db.close()
    println("DONE.")
  }
}
