package database.util

import database.MemoryBuffer

class MemorySeq(mb: MemoryBuffer, x: Long, y: Long) extends Seq[Byte] {
  outer ⇒
  val length: Int = (y - x + 1).toInt

  def apply(idx: Int): Byte = if (isDefinedAt(idx)) {
    mb.get(x + idx)
  } else {
    throw new IndexOutOfBoundsException(s"idx=$idx, length=$length")
  }

  def iterator: Iterator[Byte] = new Iterator[Byte] {
    private var i = x
    def hasNext: Boolean = i <= y
    def next(): Byte = {
      val res = mb.get(i)
      i += 1
      res
    }
  }

  override def head: Byte = mb.get(x)

  override def last: Byte = mb.get(y)

  override def tail: Seq[Byte] = drop(1)

  override def init: Seq[Byte] = dropRight(1)

  override def drop(n: Int): Seq[Byte] = if (x <= y - n) new MemorySeq(mb, x + n, y) else Nil

  override def take(n: Int): Seq[Byte] = if (x <= y - n) new MemorySeq(mb, x, x + n - 1) else this

  override def dropRight(n: Int): Seq[Byte] = if (x <= y - n) new MemorySeq(mb, x, y - n) else Nil

  override def takeRight(n: Int): Seq[Byte] = if (x <= y - n) new MemorySeq(mb, y - n + 1, y) else this

  override def slice(from: Int, until: Int): Seq[Byte] = {
    val lo = from.max(0)
    val hi = until.min(length)
    if (hi <= lo || isEmpty) Nil else new MemorySeq(mb, x + lo, x + hi - 1)
  }

  override def reverseIterator: Iterator[Byte] = new Iterator[Byte] {
    private var i = y
    def hasNext: Boolean = i >= x
    def next(): Byte = {
      val res = mb.get(i)
      i -= 1
      res
    }
  }

  override def reverse: Seq[Byte] = new ReversedMemorySeq(this)
}

class ReversedMemorySeq(ms: MemorySeq) extends Seq[Byte] {
  val length: Int = ms.length
  def apply(idx: Int): Byte = ms.apply(length - idx - 1)
  def iterator: Iterator[Byte] = ms.reverseIterator
  override def head: Byte = ms.last
  override def last: Byte = ms.head
  override def tail: Seq[Byte] = ms.dropRight(1).reverse
  override def init: Seq[Byte] = ms.drop(1).reverse
  override def drop(n: Int): Seq[Byte] = ms.dropRight(n).reverse
  override def take(n: Int): Seq[Byte] = ms.takeRight(n).reverse
  override def dropRight(n: Int): Seq[Byte] = ms.drop(n).reverse
  override def takeRight(n: Int): Seq[Byte] = ms.take(n).reverse
  override def slice(from: Int, until: Int): Seq[Byte] = {
    val lo = from.max(0)
    if (until <= lo || isEmpty) Nil else this.drop(lo).take(until - lo)
  }
  override def reverseIterator: Iterator[Byte] = ms.iterator
  override def reverse: Seq[Byte] = ms
}

