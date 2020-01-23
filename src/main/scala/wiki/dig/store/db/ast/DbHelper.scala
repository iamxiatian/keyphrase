package wiki.dig.store.db.ast

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

trait DbHelper {

  def getBytesFromFloatSeq(ids: Iterable[Float]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    dos.writeInt(ids.size)
    ids.foreach(dos.writeFloat(_))

    dos.close()
    out.close()

    out.toByteArray
  }

  def readFloatSeqFromBytes(bytes: Array[Byte]): Seq[Float] = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))
    val count = din.readInt()
    val ids = (0 until count).map(_ => din.readFloat()).toSeq
    din.close()
    ids
  }


  def getBytesFromIntSeq(ids: Iterable[Int]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    dos.writeInt(ids.size)
    ids.foreach(dos.writeInt(_))

    dos.close()
    out.close()

    out.toByteArray
  }

  def readIntSeqFromBytes(bytes: Array[Byte]): Seq[Int] = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))
    val count = din.readInt()
    val ids = (0 until count).map(_ => din.readInt()).toSeq
    din.close()
    ids
  }

  def getBytesFromStringSeq(ids: Seq[String]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    dos.writeInt(ids.size)
    ids.foreach(dos.writeUTF(_))

    dos.close()
    out.close()

    out.toByteArray
  }

  def readStringSeqFromBytes(bytes: Array[Byte]): Seq[String] = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))
    val count = din.readInt()
    val ids = (0 until count).map(_ => din.readUTF()).toSeq
    din.close()
    ids
  }

  def readSeqSizeFromBytes(bytes: Array[Byte]): Int = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))
    val count = din.readInt()
    din.close()
    count
  }
}
