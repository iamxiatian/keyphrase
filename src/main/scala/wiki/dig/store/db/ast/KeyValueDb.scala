package wiki.dig.store.db.ast

import java.io.File
import java.nio.charset.StandardCharsets

import org.rocksdb._
import org.rocksdb.util.SizeUnit
import org.slf4j.LoggerFactory

/**
  * Key-Value类型的数据库基类
  *
  * @author Tian Xia
  *         School of IRM, Renmin University of China.
  *         Aug 17, 2018 10:29
  */
class KeyValueDb(name: String, dbFile: File, readOnly: Boolean = false) {
  val LOG = LoggerFactory.getLogger(this.getClass)
  val UTF8 = StandardCharsets.UTF_8

  RocksDB.loadLibrary()

  val options = new Options().setCreateIfMissing(!readOnly)
    .setWriteBufferSize(200 * SizeUnit.MB)
    .setMaxWriteBufferNumber(3)
    .setMaxBackgroundCompactions(10)
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)


  // a factory method that returns a RocksDB instance
  val db: RocksDB = {
    val rocksDB = RocksDB.open(options, dbFile.getAbsolutePath)
    LOG.debug(s"Open RocksDB from ${dbFile.getCanonicalFile}")
    rocksDB
  }

  def put(k: Array[Byte], v: Array[Byte]) = db.put(k, v)

  def put(k: String, v: String) =
    db.put(
      k.getBytes(UTF8),
      v.getBytes(UTF8)
    )

  def put(pairs: Seq[(String, String)]) = {
    val writeOpt = new WriteOptions()
    val batch = new WriteBatch()
    pairs.foreach {
      case (k, v) =>
        batch.put(k.getBytes(UTF8), v.getBytes(UTF8))

    }
    db.write(writeOpt, batch)
  }

  def close = {
    db.close()
  }

  def get(k: Array[Byte]): Option[Array[Byte]] = {
    val v = db.get(k)
    if (v == null) None else Some(v)
  }

  def get(k: String): Option[String] = {
    val v = db.get(k.getBytes(UTF8))
    if (v == null) None else Some(new String(v, UTF8))
  }

}
