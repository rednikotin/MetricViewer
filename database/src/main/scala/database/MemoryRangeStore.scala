package database

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import database.util.BufferUtil._

object MemoryRangeStore {

}

// not thread safe! simple implementation
class MemoryRangeStore(totalSlots: Int, impl: MemoryBuffer) extends RangeApi {
  private var currSlot = 0
  private var currPos: Long = 0L
  // slot(n) - start pos of next
  private val slots = new Array[Long](totalSlots)

  private object writeLock

  def size: Int = currSlot

  def put(arr: Array[Byte]): Unit = writeLock.synchronized {
    //println(s"currPos=$currPos, arr.length=${arr.length}, rem=${impl.getSize() - arr.length - currPos}")
    impl.put(currPos, arr)
    currPos += arr.length
    slots(currSlot) = currPos
    currSlot += 1
  }

  def put(bb: ByteBuffer): Unit = writeLock.synchronized {
    val len = bb.remaining()
    impl.put(currPos, bb)
    currPos += len
    slots(currSlot) = currPos
    currSlot += 1
  }

  def putAt(bb: ByteBuffer, idx: Int): Unit = ???

  def getA(x: Int): Array[Byte] = {
    val pos = if (x == 0) 0L else slots(x - 1)
    val len = (slots(x) - pos).toInt
    val arr = new Array[Byte](len)
    impl.get(pos, arr)
    arr
  }

  def get(x: Int): ByteBuffer = {
    val arr = getA(x)
    ByteBuffer.wrap(arr)
  }

  def putRange(bb: ByteBuffer, offsets: Array[Int]): Unit = {
    val len = bb.remaining()
    impl.put(currPos, bb)
    for (i ← offsets.indices) {
      slots(currSlot) = currPos + offsets(i)
      currSlot += 1
    }
    currPos += len
    slots(currSlot) = currPos
    currSlot += 1
  }

  def getRangeS(x: Int, y: Int): Seq[Byte] = {
    if (x >= currSlot || y < 0) {
      return Nil
    }
    val nx = 0.max(x)
    val ny = y.min(currSlot - 1)
    val from = if (nx == 0) 0L else slots(nx - 1)
    val until = slots(ny)
    impl.slice(from, until)
  }

  def getRangeA(x: Int, y: Int): Array[Byte] = {
    if (x >= currSlot || y < 0) {
      return new Array[Byte](0)
    }
    val nx = 0.max(x)
    val ny = y.min(currSlot - 1)
    val pos = if (nx == 0) 0L else slots(nx - 1)
    val len = (slots(ny) - pos).toInt
    val arr = new Array[Byte](len)
    impl.get(pos, arr)
    arr
  }

  def getRange(x: Int, y: Int): ByteBuffer = {
    val arr = getRangeA(x, y)
    ByteBuffer.wrap(arr)
  }

  def getRangeIteratorA(x: Int, y: Int): Iterator[Array[Byte]] = {
    if (x >= currSlot || y < 0) {
      return Iterator.empty
    }
    val nx = 0.max(x)
    val ny = y.min(currSlot - 1)
    nx.to(ny).iterator.map(i ⇒ getA(i))
  }

  def getRangeIterator(x: Int, y: Int): Iterator[ByteBuffer] =
    getRangeIteratorA(x, y).map(ByteBuffer.wrap)

  def print(limit: Int = 100): Unit = {
    println("*" * 30)
    println(s"currSlot=$currSlot, currPos=$currPos, totalSlots=$totalSlots")
    println(s"slots=${slots.toList.take(currSlot).mkString(", ")}")
    val sz = limit.toLong.min(slots(currSlot - 1)).toInt
    val memory = new Array[Byte](sz)
    impl.get(0L, memory)
    println(s"memory=${memory.mkString(", ")}")
    var i = 0
    var sz1 = 0
    while (sz1 < limit && i < currSlot) {
      val bb = get(i)
      i += 1
      sz1 += bb.limit()
      println(s"i -> ${bb.mkString(", ")}")
    }
    println("*" * 30)
  }

  var raf: Option[RandomAccessFile] = None
}