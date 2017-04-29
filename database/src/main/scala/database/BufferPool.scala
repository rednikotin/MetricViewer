package database

import java.nio.ByteBuffer

import com.typesafe.scalalogging.LazyLogging

// todo: improve implementation
class BufferPool extends LazyLogging {
  val step = 4096
  private var cnt = 0
  private var totalSize = 0L
  private var totalTime = 0L
  private def updStats(size: Int, time: Long): Unit = {
    cnt += 1
    totalSize += size
    totalTime += time
    if (cnt % 100 == 2) logger.debug(s"STATS: cnt=$cnt, totalSize=$totalSize, totalTime=$totalTime")
  }

  val zero: ByteBuffer = ByteBuffer.allocate(0)

  def allocate(minSize: Int): ByteBuffer = if (minSize == 0) {
    zero
  } else {
    val size = (minSize / step + 1) * step
    val t0 = System.nanoTime()
    val bb = ByteBuffer.allocateDirect(size)
    val t1 = System.nanoTime()
    updStats(size, t1 - t0)
    bb.limit(minSize)
    bb
  }

  def release(bb: ByteBuffer): Unit = {
    // gc will take care so far, no reuse :-(
  }
}
