package wiki.dig.expt

import java.io.File
import java.nio.charset.StandardCharsets
import java.text.DecimalFormat

import breeze.linalg.DenseVector
import com.google.common.collect.Lists
import com.google.common.io.Files
import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyHandle, DBOptions, RocksDB}
import org.slf4j.LoggerFactory
import wiki.dig.MyConf
import wiki.dig.store.db.ast.{Db, DbHelper}
import wiki.dig.store.db.{CategoryDb, PageContentDb}
import wiki.dig.util.ByteUtil

import scala.io.Source

/**
  * 试验分析用的临时数据库，后续会删除
  *
  */
object ExptDb extends Db with DbHelper {
  val LOG = LoggerFactory.getLogger(this.getClass)

  import StandardCharsets.UTF_8

  val dbPath = new File(MyConf.dbRootDir, "expt")
  if (!dbPath.getParentFile.exists())
    dbPath.getParentFile.mkdirs()

  RocksDB.loadLibrary()

  val options = new DBOptions().setCreateIfMissing(true)
    .setMaxBackgroundCompactions(10)
    .setCreateMissingColumnFamilies(true)

  protected val cfNames = Lists.newArrayList[ColumnFamilyDescriptor](
    new ColumnFamilyDescriptor("default".getBytes(UTF_8)), //默认为GloVE 100
    new ColumnFamilyDescriptor("skipped".getBytes(UTF_8))
  )

  protected val cfHandlers = Lists.newArrayList[ColumnFamilyHandle]

  protected val db = RocksDB.open(options, dbPath.getAbsolutePath, cfNames, cfHandlers)

  protected val defaultHandler: ColumnFamilyHandle = cfHandlers.get(0)
  protected val skippedHandler: ColumnFamilyHandle = cfHandlers.get(1)

  def buildArticleEmbedding(articleIdFile: File = new File("./sample.page.ids.txt")) = {
    val pageEmbeddingWriter = Files.newWriter(new File("sample.page.embedding.txt"), UTF_8)
    val source = Source.fromFile(articleIdFile, "UTF-8")

    val f = new DecimalFormat("0.#####")
    source.getLines().filter(line => line.nonEmpty && !line.startsWith("#"))
      .zipWithIndex
      .foreach {
        case (line, idx) =>
          val pid = line.toInt
          PageContentDb.getContent(pid).foreach {
            content =>
              //只保留前面的10000个字符，进行分析
              val text =
                if (content.length > 10000) {
                  val pos = content.indexOf(".", 8000)
                  // 为避免单词被截断，保留最后一个空格之前的内容
                  if (pos > 0) content.substring(0, pos) else content
                } else content

              //太短的文章忽略
              if (text.length > 500) {
                val v = calculateVector(text)
                db.put(defaultHandler,
                  ByteUtil.int2bytes(pid),
                  getBytesFromFloatSeq(v.data))

                val stringValues = v.data.map(f.format(_)).mkString(" ")
                pageEmbeddingWriter.write(s"$pid ${stringValues}\n")
              } else {
                println(s"SKIP $pid")
                db.put(skippedHandler,
                  ByteUtil.int2bytes(pid),
                  ByteUtil.int2bytes(text.length)
                )
              }
          }
          if (idx % 500 == 0) {
            println(idx)
            pageEmbeddingWriter.flush()
          }
      }
    source.close()
    pageEmbeddingWriter.close()
    println("DONE")
  }

  def calculateVector(text: String): DenseVector[Float] = {
    //分词，并转换为词语的embedding表示的vector列表
    val vectors: Seq[DenseVector[Float]] = text.split(" |\t|\n|\r|\"").filter(_.nonEmpty).map {
      token =>
        if ((token.endsWith(".") && token.indexOf(".") == token.length - 1) ||
          token.endsWith(",") ||
          token.endsWith("!") ||
          token.endsWith("?")
        )
          token.substring(0, token.length - 1)
        else
          token
    }.flatMap {
      token =>
        EmbeddingDb.find(token.toLowerCase).map {
          v => DenseVector(v.toArray)
        }
    }

    //取平均值作为文本的embedding结果
    val total = DenseVector.zeros[Float](vectors(0).length)
    vectors.foreach {
      v =>
        total += v
    }

    val count: Float = vectors.size
    if (count > 0) total /= count else total
  }

  def find(id: Int): Option[Seq[Float]] =
    Option(db.get(defaultHandler, ByteUtil.int2bytes(id))).map {
      readFloatSeqFromBytes(_)
    }

  def outCategoryNames() = {
    val source = Source.fromFile(new File("sample.category.ids.txt"), "UTF-8")
    val writer = Files.newWriter(new File("sample.category.names.txt"), UTF_8)
    source.getLines().filter(_.nonEmpty)
      .foreach {
        id =>
          CategoryDb.getNameById(id.toInt).foreach {
            name =>
              writer.write(s"$id ${name}\n")
          }
      }
    writer.close()
    source.close()
    println("DONE")
  }

  /**
    * 数据库名字
    */
  def dbName: String = "Expt DB"

  override def close(): Unit = {
    print(s"==> Close $dbName ... ")
    cfHandlers.forEach(h => h.close())
    db.close()
    println("DONE.")
  }

  def main(args: Array[String]): Unit = {
    outCategoryNames()
  }
}
