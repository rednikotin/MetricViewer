package database

import java.nio.ByteBuffer

class ByteBufferSeq(bb: ByteBuffer, x: Int, y: Int) extends Seq[Byte] {
  outer ⇒
  val length: Int = (y - x + 1).toInt

  def apply(idx: Int): Byte = if (idx < length && idx >= 0) {
    bb.get(x + idx)
  } else {
    throw new IndexOutOfBoundsException(s"idx=$idx, length=$length")
  }

  def iterator: Iterator[Byte] = new Iterator[Byte] {
    private var i = x
    def hasNext: Boolean = i <= y
    def next(): Byte = {
      val res = bb.get(i)
      i += 1
      res
    }
  }

  override def head: Byte = bb.get(x)

  override def last: Byte = bb.get(y)

  override def tail: Seq[Byte] = drop(1)

  override def init: Seq[Byte] = dropRight(1)

  override def drop(n: Int): Seq[Byte] = if (x <= y - n) new ByteBufferSeq(bb, x + n, y) else Nil

  override def take(n: Int): Seq[Byte] = if (x <= y - n) new ByteBufferSeq(bb, x, x + n - 1) else this

  override def dropRight(n: Int): Seq[Byte] = if (x <= y - n) new ByteBufferSeq(bb, x, y - n) else Nil

  override def takeRight(n: Int): Seq[Byte] = if (x <= y - n) new ByteBufferSeq(bb, y - n + 1, y) else this

  override def slice(from: Int, until: Int): Seq[Byte] = {
    val lo = from.max(0)
    val hi = until.min(length - 1)
    if (hi <= lo || isEmpty) Nil else new ByteBufferSeq(bb, x + lo, x + hi - 1)
  }

  override def reverseIterator: Iterator[Byte] = new Iterator[Byte] {
    private var i = y
    def hasNext: Boolean = i >= x
    def next(): Byte = {
      val res = bb.get(i)
      i -= 1
      res
    }
  }

  override def reverse: Seq[Byte] = new ReversedByteBufferSeq(this)
}

class ReversedByteBufferSeq(bbs: ByteBufferSeq) extends Seq[Byte] {
  val length: Int = bbs.length
  def apply(idx: Int): Byte = bbs.apply(length - idx - 1)
  def iterator: Iterator[Byte] = bbs.reverseIterator
  override def head: Byte = bbs.last
  override def last: Byte = bbs.head
  override def tail: Seq[Byte] = bbs.dropRight(1).reverse
  override def init: Seq[Byte] = bbs.drop(1).reverse
  override def drop(n: Int): Seq[Byte] = bbs.dropRight(n).reverse
  override def take(n: Int): Seq[Byte] = bbs.takeRight(n).reverse
  override def dropRight(n: Int): Seq[Byte] = bbs.drop(n).reverse
  override def takeRight(n: Int): Seq[Byte] = bbs.take(n).reverse
  override def slice(from: Int, until: Int): Seq[Byte] = {
    val lo = from.max(0)
    if (until <= lo || isEmpty) Nil else this.drop(lo).take(until - lo)
  }
  override def reverseIterator: Iterator[Byte] = bbs.iterator
  override def reverse: Seq[Byte] = bbs
}
