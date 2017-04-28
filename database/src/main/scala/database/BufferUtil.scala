package database

import java.nio.ByteBuffer

object BufferUtil {
  implicit class RichByteBuffer(bb: ByteBuffer) {
    def toSeq: Seq[Byte] = new Seq[Byte] {
      private val initPos = bb.position()
      val length: Int = bb.remaining()
      def apply(idx: Int): Byte = bb.get(idx)
      def iterator: Iterator[Byte] = new Iterator[Byte] {
        bb.position(initPos)
        def hasNext: Boolean = bb.hasRemaining
        def next(): Byte = bb.get()
      }
    }
  }

  implicit class RichMemoryBuffer(mb: MemoryBuffer) {
    def slice(from: Long, until: Long): Seq[Byte] = {
      val x: Long = from.max(0).min(mb.getSize)
      val y: Long = (until - 1).max(0).min(mb.getSize)
      new MemorySeq(mb, x, y)
    }
  }

}
