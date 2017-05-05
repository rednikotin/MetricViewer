package database

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging
import database.BufferPool.PoolProvider

object BufferPool {
  final val step = 4096
  final val MIN_SHIFT: Int = 12
  final val MAX_SHIFT: Int = 24
  final val MIN_SIZE: Int = 1 << MIN_SHIFT
  final val MAX_SIZE: Int = 1 << MAX_SHIFT
  final val MAX_POOL_SIZE = 512 * 128

  trait Pool {
    def tryGet(): ByteBuffer
    def release(bb: ByteBuffer): Unit
    def getSize: Int
    def printStats(): Unit = {}
  }

  trait PoolProvider {
    def create(): Pool
  }

  // simple blocking
  object BPool extends PoolProvider {
    def create(): BPool = new BPool(MAX_POOL_SIZE)
  }
  class BPool(maxSize: Int) extends Pool {
    @volatile private final var size = 0
    private val stack = new Array[ByteBuffer](maxSize)
    def tryGet(): ByteBuffer = {
      if (size == 0) {
        null
      } else {
        synchronized {
          if (size == 0) {
            null
          } else {
            size -= 1
            stack(size)
          }
        }
      }
    }
    def release(bb: ByteBuffer): Unit = this.synchronized {
      if (size < maxSize) {
        stack(size) = bb
        size += 1
      }
    }
    def getSize: Int = size
  }

  object NBPool extends PoolProvider {
    def create(): NBPool = new NBPool(MAX_POOL_SIZE)
  }
  class NBPool(maxSize: Int) extends Pool {
    private final val get0Empty = new AtomicInteger(0)
    private final val getOk = new AtomicInteger(0)
    private final val getMiss = new AtomicInteger(0)
    private final val get0Miss = new AtomicInteger(0)
    private final val releaseMiss = new AtomicInteger(0)

    private final val head = new AtomicInteger(0)
    private final val tail = new AtomicInteger(0)
    private val data = new Array[ByteBuffer](maxSize)
    def tryGet0(): ByteBuffer = {
      val curHead = head.get
      val curTail = tail.get
      val nextTail = (curTail + 1) % maxSize
      if (curTail != curHead) {
        val res = data(curTail)
        if (tail.compareAndSet(curTail, nextTail)) {
          getOk.incrementAndGet()
          res
        } else {
          get0Miss.incrementAndGet()
          null
        }
      } else {
        get0Empty.incrementAndGet()
        null
      }
    }
    def tryGet(): ByteBuffer = {
      var res = tryGet0()
      var i = 0
      while (res == null && i < 10) {
        i += 1
        res = tryGet0()
      }
      if (res == null) getMiss.incrementAndGet()
      res
    }
    def release(bb: ByteBuffer): Unit = {
      val curTail = tail.get
      val curHead = head.get
      val nextHead = (curHead + 1) % maxSize
      if (curTail != nextHead) {
        bb.clear()
        data(curHead) = bb
        if (!head.compareAndSet(curHead, nextHead)) {
          releaseMiss.incrementAndGet()
        }
      } else {
        releaseMiss.incrementAndGet()
      }
    }
    def getSize: Int = {
      val curHead = head.get
      val curTail = tail.get
      if (curTail <= curHead) curHead - curTail else maxSize - curTail + curHead
    }
    override def printStats(): Unit = {
      println(s"releaseMiss=${releaseMiss.get}, getMiss=${getMiss.get}, get0Miss=${get0Miss.get}, getOk=${getOk.get}, get0Empty=${get0Empty.get}")
    }
  }

  // wrapper for ConcurrentLinkedQueue
  object CLQPool extends PoolProvider {
    def create(): CLQPool = new CLQPool(MAX_POOL_SIZE)
  }
  class CLQPool(maxSize: Int) extends Pool {
    private val queue = new java.util.concurrent.ConcurrentLinkedQueue[ByteBuffer]()
    def tryGet(): ByteBuffer = queue.poll()
    def release(bb: ByteBuffer): Unit = {
      if (queue.size() < maxSize) queue.offer(bb)
    }
    def getSize: Int = queue.size()
  }

  def createBufferPool(poolProvider: PoolProvider): BufferPool = new BufferPool(poolProvider)

  def createDefault(): BufferPool = new BufferPool(BPool)
}

class BufferPool(poolProvider: PoolProvider) extends LazyLogging {
  import BufferPool._

  private val miss = new AtomicInteger(0)
  private val pools: Array[Pool] =
    (MIN_SHIFT to MAX_SHIFT).map(i ⇒ poolProvider.create()).toArray

  private def poolIdx(i: Int): Int = 32 - Integer.numberOfLeadingZeros(i - 1) - MIN_SHIFT
  def zero: ByteBuffer = ByteBuffer.allocate(0)

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
  def warmUp(): Unit = {
    for ((shift, idx) ← MAX_SHIFT.to(MIN_SHIFT, -1).zipWithIndex) {
      warmUp(shift, (1 << idx).min(MAX_POOL_SIZE))
    }
    warmUp(MIN_SHIFT, MAX_POOL_SIZE)
    //logger.debug("PREALLOCATED POOLS: " + pools.map(_.getSize).mkString(", "))
  }
  //warmUp()
  def getMiss: Int = miss.get()
  def printStats(): Unit = pools.headOption.foreach(_.printStats())
}
