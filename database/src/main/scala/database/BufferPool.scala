package database

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging

trait BufferPool {
  def allocate(minSize: Int): ByteBuffer
  def release(bb: ByteBuffer): Unit
  def warmUp(): Unit
  def getMiss: Int
}

object BufferPool {
  final val step = 4096
  final val POOL_CONFIG = Array((12, 10), (13, 10), (14, 10), (15, 9), (16, 8), (17, 7), (18, 7), (19, 6), (20, 5), (21, 5), (22, 4), (23, 4), (24, 4))
  final val MIN_SHIFT: Int = POOL_CONFIG.head._1
  final val MIN_SIZE: Int = 1 << MIN_SHIFT
  final val MAX_SHIFT: Int = POOL_CONFIG.last._1
  final val MAX_SIZE: Int = 1 << MAX_SHIFT
  final val MAX_RETRIES: Int = 32

  def apply(): BufferPool = new BufferPoolImpl(BPool)
  def newNBPool(): BufferPool = new BufferPoolImpl(NBPool)
  def newBPool(): BufferPool = new BufferPoolImpl(BPool)

  private class BufferPoolImpl(poolFactory: PoolFactory) extends BufferPool with LazyLogging {
    private val miss = new AtomicInteger(0)
    private val pools: Array[Pool] = POOL_CONFIG.map(i ⇒ poolFactory.newInstance(1 << i._2))
    private def poolIdx(i: Int): Int = 32 - Integer.numberOfLeadingZeros(i - 1) - MIN_SHIFT
    private def zero: ByteBuffer = ByteBuffer.allocate(0)

    def allocate(minSize: Int): ByteBuffer = if (minSize == 0) {
      zero
    } else {
      val idx = if (minSize <= MIN_SIZE) 0 else poolIdx(minSize)
      val pool = pools(idx)
      var bb: ByteBuffer = pool.tryGet()
      if (bb == null) {
        miss.incrementAndGet()
        val size = 1 << (idx + MIN_SHIFT)
        bb = ByteBuffer.allocateDirect(size)
      }
      bb.limit(minSize)
      bb
    }

    def release(bb: ByteBuffer): Unit = {
      val capacity = bb.capacity()
      if (capacity >= MIN_SIZE && capacity <= MAX_SIZE) {
        val idx = poolIdx(capacity)
        val size = 1 << (idx + MIN_SHIFT)
        if (size != capacity) {
          logger.warn(s"trying to release buffer with weird capacity = $capacity")
        } else {
          bb.clear()
          val pool = pools(idx)
          pool.release(bb)
        }
      }
    }

    private def warmUp(shift: Int, size: Int): Unit = (1 to size).map(_ ⇒ ByteBuffer.allocateDirect(1 << shift)).foreach(release)
    def warmUp(): Unit = for ((s, ps) ← POOL_CONFIG) warmUp(s, 1 << ps)
    //warmUp()
    def getMiss: Int = miss.get()
  }

  private trait Pool {
    def tryGet(): ByteBuffer
    def release(bb: ByteBuffer): Unit
  }

  private trait PoolFactory {
    def newInstance(maxSize: Int): Pool
  }

  private object NBPool extends PoolFactory {
    def newInstance(maxSize: Int): NBPool = new NBPool(maxSize)
  }

  private class NBPool(maxSize: Int) extends Pool {
    assert((maxSize & (maxSize - 1)) == 0)
    private final val modulo = maxSize - 1
    private final val head = new AtomicInteger(0)
    private final val tail = new AtomicInteger(0)
    private final val data = new Array[ByteBuffer](maxSize)

    private def tryGet0(): ByteBuffer = {
      // order makes sense!!
      val curTail = tail.get
      val curHead = head.get
      val nextTail = curTail + 1
      if ((curTail & modulo) != (curHead & modulo)) {
        val res = data(nextTail & modulo)
        if (tail.compareAndSet(curTail, nextTail)) {
          res
        } else null
      } else null
    }

    def tryGet(): ByteBuffer = {
      var res = tryGet0()
      var i = 0
      while (res == null && i < MAX_RETRIES) {
        Thread.`yield`()
        i += 1
        res = tryGet0()
      }
      res
    }

    def release(bb: ByteBuffer): Unit = {
      val curTail = tail.get
      val curHead = head.get
      val nextHead = curHead + 1
      if ((nextHead & modulo) != (curTail & modulo)) {
        bb.clear()
        data(nextHead & modulo) = bb
        head.compareAndSet(curHead, nextHead)
      }
    }
  }

  // simple blocking
  private object BPool extends PoolFactory {
    def newInstance(maxSize: Int): BPool = new BPool(maxSize)
  }

  private class BPool(maxSize: Int) extends Pool {
    @volatile private final var size = 0
    private val data = new Array[ByteBuffer](maxSize)
    def tryGet(): ByteBuffer = {
      if (size == 0) {
        null
      } else {
        synchronized {
          if (size == 0) {
            null
          } else {
            size -= 1
            data(size)
          }
        }
      }
    }
    def release(bb: ByteBuffer): Unit = this.synchronized {
      if (size < maxSize) {
        data(size) = bb
        size += 1
      }
    }
  }

}
