package database

import java.nio.ByteBuffer

object BufferUtil {
  implicit def richByteBuffer(bb: ByteBuffer): Seq[Byte] =
    new ByteBufferSeq(bb, 0, bb.limit() - 1)

  implicit class RichMemoryBuffer(mb: MemoryBuffer) {
    def slice(from: Long, until: Long): Seq[Byte] = {
      val x: Long = from.max(0).min(mb.getSize)
      val y: Long = (until - 1).max(0).min(mb.getSize)
      new MemorySeq(mb, x, y)
    }
  }

}
