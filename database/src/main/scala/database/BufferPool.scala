package database

import java.nio.ByteBuffer

import com.typesafe.scalalogging.LazyLogging

class BufferPool extends LazyLogging {
  val step = 4096
  private var cnt = 0L
  private var totalSize = 0L
  private var totalTime = 0L
  private var logCnt = 5L
  private def updStats(size: Int, time: Long): Unit = {
    cnt += 1
    totalSize += size
    totalTime += time
    if (cnt == logCnt) {
      logCnt = logCnt * 2
      logger.debug(
        s"STATS: cnt=$cnt, " +
          s"totalSize=$totalSize, " +
          s"totalTime=$totalTime, " +
          s"avg=${totalTime / cnt}, " +
          s"avg1kb=${1024L * totalTime / totalSize} " +
          s"avgSize=${totalSize / cnt}"
      )
    }
  }

  val zero: ByteBuffer = ByteBuffer.allocate(0)

  //private val sizes

  def allocate(minSize: Int): ByteBuffer = if (minSize == 0) {
    zero
  } else {
    val size = if (minSize % step == 0) minSize else (minSize / step + 1) * step
    val t0 = System.nanoTime()
    val bb = ByteBuffer.allocateDirect(size)
    val t1 = System.nanoTime()
    updStats(size, t1 - t0)
    bb.limit(minSize)
    bb
  }

  // should be called after buffer no more needed, potential issue if buffer accessed after released
  // challenge to find correct place to release buffer: if we read buffer and send it over the websocket
  // we have to release it after io completed, i.e. on message sent
  // benefits are not clear. Allocate 4mb is about milliseconds by tests (weird though)
  def release(bb: ByteBuffer): Unit = {
    // gc will take care so far, no reuse :-(
  }

  def resetStat(): Unit = {
    cnt = 0L
    totalSize = 0L
    totalTime = 0L
    logCnt = 5L
  }
}
