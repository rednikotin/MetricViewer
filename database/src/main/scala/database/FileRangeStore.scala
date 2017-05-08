package database

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.{AsynchronousFileChannel, CompletionHandler, FileChannel}
import java.nio.file.StandardOpenOption
import BufferUtil._
import com.typesafe.scalalogging.LazyLogging
import sun.nio.ch.DirectBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import UnsafeUtil._

// todo: cleanup after failed write + other error handling
// done: bulk load api - putRange/putRangeAt API
// done: sorting buffer
// todo: review concurrency issue
// todo: create version with unsafe instead?

object FileRangeStore {
  final val STEP: Int = 4096
  final val RESERVED_LIMIT: Int = 1073741824
  final val MAX_POS: Int = Int.MaxValue
  final val MAX_WRITE_QUEUE: Int = 100

  class MMInt(mmap: MappedByteBuffer, addr: Int, initValue: Option[Int]) {
    @volatile private var cache: Int = _
    def get: Int = cache
    def set(value: Int): Unit = this.synchronized {
      cache = value
      mmap.putInt(addr, value)
    }
    def +=(delta: Int): Int = this.synchronized {
      cache += delta
      mmap.putInt(addr, cache)
      cache
    }
    initValue match {
      case Some(value) ⇒ set(value)
      case None        ⇒ cache = mmap.getInt(addr)
    }
  }

  case class GetResult(result: Int, buffer: ByteBuffer)
  case class PutResult[T](slot: Int, result: T)

  implicit class RichAsynchronousFileChannel(async: AsynchronousFileChannel) {
    def putX(buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit): Future[GetResult] = {
      val p = Promise[GetResult]()
      async.write(buffer, pos, buffer, new CompletionHandler[Integer, ByteBuffer]() {
        override def completed(result: Integer, attachment: ByteBuffer): Unit = {
          callback(true)
          p.success(GetResult(result.toInt, attachment))
        }
        override def failed(exc: Throwable, attachment: ByteBuffer): Unit = {
          callback(false)
          p.failure(exc)
        }
      })
      p.future
    }
    def getX(buffer: ByteBuffer, pos: Int): Future[ByteBuffer] = {
      val p = Promise[ByteBuffer]()
      async.read(buffer, pos, buffer, new CompletionHandler[Integer, ByteBuffer]() {
        override def completed(result: Integer, attachment: ByteBuffer): Unit = {
          attachment.flip()
          p.success(attachment)
        }
        override def failed(exc: Throwable, attachment: ByteBuffer): Unit = p.failure(exc)
      })
      p.future
    }
  }

  implicit class RichFileChannel(channel: FileChannel) {
    def putX(buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit): Int = {
      val res = Try(channel.write(buffer, pos))
      callback(res.isSuccess)
      res.get
    }
    def getX(buffer: ByteBuffer, pos: Int): ByteBuffer = {
      channel.read(buffer, pos)
      buffer
    }
  }

  implicit class RichMappedByteBuffer(mmap: MappedByteBuffer) {
    def putX0(buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit): Int = {
      val res = Try {
        mmap.synchronized {
          mmap.position(pos)
          mmap.put(buffer)
          mmap.position() - pos
        }
      }
      callback(res.isSuccess)
      res.get
    }
    private val addr = getBufferAddress(mmap)
    private val unsafe = getUnsafe
    def putX(buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit): Int = {
      val len = buffer.limit()
      if (len > 0) {
        val res = Try {
          buffer match {
            case bb: DirectBuffer ⇒
              val saddr = getBufferAddress(bb)
              unsafe.copyMemory(saddr, addr + pos, len)
            case bb: ByteBuffer ⇒
              val array = bb.array()
              unsafe.copyMemory(array, BYTE_ARRAY_OFFSET, null, addr + pos, len)
          }
          len
        }
        callback(res.isSuccess)
        res.get
      } else {
        callback(true)
        0
      }
    }
  }

  class FileRangeStoreException(val msg: String) extends RuntimeException(msg: String)
  class WriteQueueOverflowException(msg: String) extends FileRangeStoreException(msg: String)
  class WriteBeyondMaxPosException(msg: String) extends FileRangeStoreException(msg: String)
  class NoAvailableSlotsException(msg: String) extends FileRangeStoreException(msg: String)
  class SlotAlreadyUsedException(msg: String) extends FileRangeStoreException(msg: String)
  class NegativeSlotException(msg: String) extends FileRangeStoreException(msg: String)
  class ReadAboveWatermarkException(msg: String) extends FileRangeStoreException(msg: String)
  class MmapWriteNotEnabled extends FileRangeStoreException("MMAP writes are not explicitly enabled")
}

class FileRangeStore(val file: File, val totalSlots: Int) extends RangeAsyncApi with LazyLogging {
  import FileRangeStore._
  /*
       Small file structure:
       0 - 64K => reserved for control needs
       64k - 64k + 4 * slots => offsets
       64k + 4 * slots => data
  */

  final val SLOTS_SIZE: Int = if (totalSlots % 1024 == 0) totalSlots * 4 else (totalSlots * 4 / STEP + 1) * STEP
  final val SLOTS_LIMIT: Int = RESERVED_LIMIT + SLOTS_SIZE

  protected val fileCreated: Boolean = !file.exists()
  val raf: RandomAccessFile = new RandomAccessFile(file, "rw")
  val channel: FileChannel = raf.getChannel
  val async: AsynchronousFileChannel = AsynchronousFileChannel.open(
    file.toPath,
    StandardOpenOption.READ,
    StandardOpenOption.WRITE,
    StandardOpenOption.CREATE
  )

  protected val bufferPool = BufferPool()
  def releaseBuffer(bb: ByteBuffer): Unit = bufferPool.release(bb)
  protected val reserved_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, RESERVED_LIMIT)
  private val slots_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, RESERVED_LIMIT, SLOTS_SIZE)
  private var data_mmap: MappedByteBuffer = _

  reserved_mmap.load()
  slots_mmap.load()

  def commitReserved(): Unit = reserved_mmap.force()
  def commitSlots(): Unit = slots_mmap.force()
  def commitAll(): Unit = async.force(false)

  private var mmapWrite = false
  def enableMmapWrite(): Unit = this.synchronized {
    if (!mmapWrite) {
      mmapWrite = true
      val size = MAX_POS - SLOTS_LIMIT + 1
      data_mmap = channel.map(FileChannel.MapMode.READ_WRITE, SLOTS_LIMIT, size)
    }
  }
  private def mmapPut(byteBuffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit): Int =
    data_mmap.putX(byteBuffer, pos - SLOTS_LIMIT, callback)

  protected val currSlot = new MMInt(reserved_mmap, 0, if (fileCreated) Some(0) else None)
  private val currPos = new MMInt(reserved_mmap, 4, if (fileCreated) Some(SLOTS_LIMIT) else None)
  private val slots = slots_mmap.asIntBuffer()
  @volatile private var readWatermark = if (fileCreated) -1 else currSlot.get - 1

  protected object writeLock
  private object queueLock

  def size: Int = currSlot.get

  // for test usage
  @volatile var maxQueueSize = 0
  @volatile private var queueSize = 0
  private val writeQueue = scala.collection.mutable.SortedSet.empty[Int]
  private def encwq(slot: Int): Unit = queueLock.synchronized {
    if (queueSize >= MAX_WRITE_QUEUE) {
      throw new WriteQueueOverflowException(s"writeQueue.size=${writeQueue.size} >= MAX_WRITE_QUEUE=$MAX_WRITE_QUEUE")
    }
    writeQueue.add(slot)
    queueSize += 1
    if (maxQueueSize < queueSize) {
      maxQueueSize = queueSize
    }
  }

  private def deqwq(slot: Int): Unit = queueLock.synchronized {
    writeQueue.remove(slot)
    queueSize -= 1
    val minSlot = writeQueue.headOption.getOrElse((slot + 1).max(currSlot.get))
    readWatermark = minSlot - 1
  }
  def getQueueSize: Int = queueSize

  // not a sync method (but still accessing volatiles), we have to read readWatermark as it may be updated during the call
  def rwGap: Int = {
    val rwm = readWatermark
    val curr = currSlot.get
    curr - rwm - 1
  }
  def getReadWatermark: Int = readWatermark

  private def put[T](bb: ByteBuffer, putf: (ByteBuffer, Int, Boolean ⇒ Unit) ⇒ T): PutResult[T] = writeLock.synchronized {
    val len = bb.remaining()
    val slot = currSlot.get
    val pos = currPos.get
    val nextPos = pos.toLong + len
    if (nextPos > MAX_POS) {
      throw new WriteBeyondMaxPosException(s"nextPos=$nextPos > MAX_POS=$MAX_POS")
    }
    if (slot >= totalSlots) {
      throw new NoAvailableSlotsException(s"currSlot.get=$slot >= totalSlots=$totalSlots")
    }
    encwq(slot)
    currPos.set(nextPos.toInt)
    slots.put(slot, nextPos.toInt)
    currSlot += 1
    val res = putf(bb, pos, _ ⇒ deqwq(slot))
    PutResult(slot, res)
  }

  private def putAt[T](bb: ByteBuffer, idx: Int, putf: (ByteBuffer, Int, Boolean ⇒ Unit) ⇒ T): PutResult[T] = writeLock.synchronized {
    if (idx >= totalSlots) {
      throw new NoAvailableSlotsException(s"idx=$idx >= totalSlots=$totalSlots")
    }
    if (idx < 0) {
      throw new NegativeSlotException(s"idx=$idx")
    }
    if (idx < currSlot.get) {
      throw new SlotAlreadyUsedException(s"idx=$idx < currSlot.get=${currSlot.get}")
    }
    while (currSlot.get < idx) {
      slots.put(currSlot.get, currPos.get)
      currSlot += 1
    }
    put(bb, putf)
  }

  // offsets: end position of all data elements in buffer
  private def putRange[T](bb: ByteBuffer, offsets: Array[Int], putf: (ByteBuffer, Int, Boolean ⇒ Unit) ⇒ T): PutResult[T] = writeLock.synchronized {
    val len = bb.remaining()
    val pos = currPos.get
    val nextPos = currPos.get.toLong + len
    val firstSlot = currSlot.get
    val maxSlot = firstSlot - 1 + offsets.length
    if (nextPos > MAX_POS) {
      throw new WriteBeyondMaxPosException(s"nextPos=$nextPos > MAX_POS=$MAX_POS")
    }
    if (maxSlot >= totalSlots) {
      throw new NoAvailableSlotsException(s"(currSlot.get=${currSlot.get} - 1 + offsets.length=${offsets.length})=maxSlot=$maxSlot >= totalSlots=$totalSlots")
    }
    encwq(maxSlot)
    for (i ← offsets.indices) {
      slots.put(currSlot.get, currPos.get + offsets(i))
      currSlot += 1
    }
    currPos += len
    val res = putf(bb, pos, _ ⇒ deqwq(maxSlot))
    PutResult(firstSlot, res)
  }

  private def putRangeAt[T](bb: ByteBuffer, offsets: Array[Int], idx: Int, putf: (ByteBuffer, Int, Boolean ⇒ Unit) ⇒ T): PutResult[T] = writeLock.synchronized {
    val len = bb.remaining()
    val pos = currPos.get
    val nextPos = currPos.get.toLong + len
    val firstSlot = currSlot.get
    val maxSlot = idx - 1 + offsets.length
    if (nextPos > MAX_POS) {
      throw new WriteBeyondMaxPosException(s"nextPos=$nextPos > MAX_POS=$MAX_POS")
    }
    if (maxSlot >= totalSlots) {
      throw new NoAvailableSlotsException(s"(idx=$idx - 1 + offsets.length=${offsets.length})=maxSlot=$maxSlot >= totalSlots=$totalSlots")
    }
    if (idx < 0) {
      throw new NegativeSlotException(s"idx=$idx")
    }
    if (idx < currSlot.get) {
      throw new SlotAlreadyUsedException(s"idx=$idx < currSlot.get=${currSlot.get}")
    }
    while (currSlot.get < idx) {
      slots.put(currSlot.get, currPos.get)
      currSlot += 1
    }
    encwq(maxSlot)
    for (i ← offsets.indices) {
      slots.put(currSlot.get, currPos.get + offsets(i))
      currSlot += 1
    }
    currPos += len
    val res = putf(bb, pos, _ ⇒ deqwq(maxSlot))
    PutResult(firstSlot, res)
  }

  def putAsync(bb: ByteBuffer): PutResult[Future[GetResult]] = put(bb, async.putX)
  def putSync(bb: ByteBuffer): PutResult[Int] = put(bb, channel.putX)
  def putSyncMmap(bb: ByteBuffer): PutResult[Int] = if (mmapWrite) put(bb, mmapPut) else throw new MmapWriteNotEnabled
  def put(bb: ByteBuffer): PutResult[Int] = putSync(bb)

  def putAtAsync(bb: ByteBuffer, idx: Int): PutResult[Future[GetResult]] = putAt(bb, idx, async.putX)
  def putAtSync(bb: ByteBuffer, idx: Int): PutResult[Int] = putAt(bb, idx, channel.putX)
  def putAtSyncMmap(bb: ByteBuffer, idx: Int): PutResult[Int] = if (mmapWrite) putAt(bb, idx, mmapPut) else throw new MmapWriteNotEnabled
  def putAt(bb: ByteBuffer, idx: Int): PutResult[Int] = putAtSync(bb, idx)

  def putRangeAsync(bb: ByteBuffer, offsets: Array[Int]): PutResult[Future[GetResult]] = putRange(bb, offsets, async.putX)
  def putRangeSync(bb: ByteBuffer, offsets: Array[Int]): PutResult[Int] = putRange(bb, offsets, channel.putX)
  def putRangeSyncMmap(bb: ByteBuffer, offsets: Array[Int]): PutResult[Int] = if (mmapWrite) putRange(bb, offsets, mmapPut) else throw new MmapWriteNotEnabled
  def putRange(bb: ByteBuffer, offsets: Array[Int]): PutResult[Int] = putRangeSync(bb, offsets)

  def putRangeAtAsync(bb: ByteBuffer, offsets: Array[Int], idx: Int): PutResult[Future[GetResult]] = putRangeAt(bb, offsets, idx, async.putX)
  def putRangeAtSync(bb: ByteBuffer, offsets: Array[Int], idx: Int): PutResult[Int] = putRangeAt(bb, offsets, idx, channel.putX)
  def putRangeAtSyncMmap(bb: ByteBuffer, offsets: Array[Int], idx: Int): PutResult[Int] = if (mmapWrite) putRangeAt(bb, offsets, idx, mmapPut) else throw new MmapWriteNotEnabled
  def putRangeAt(bb: ByteBuffer, offsets: Array[Int], idx: Int): PutResult[Int] = putRangeAtSync(bb, offsets, idx)

  // write in Future asyncs
  def putFAsync(bb: ByteBuffer)(implicit executionContext: ExecutionContext): PutResult[Future[GetResult]] =
    put(bb, (buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit) ⇒ Future(async.putX(buffer, pos, callback)).flatten)
  def putFAsyncMmap(bb: ByteBuffer)(implicit executionContext: ExecutionContext): PutResult[Future[Int]] =
    if (mmapWrite) {
      put(bb, (buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit) ⇒ Future(mmapPut(buffer, pos, callback)))
    } else {
      throw new MmapWriteNotEnabled
    }
  def putAtFAsync(bb: ByteBuffer, idx: Int)(implicit executionContext: ExecutionContext): PutResult[Future[GetResult]] =
    putAt(bb, idx, (buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit) ⇒ Future(async.putX(buffer, pos, callback)).flatten)
  def putAtFAsyncMmap(bb: ByteBuffer, idx: Int)(implicit executionContext: ExecutionContext): PutResult[Future[Int]] =
    if (mmapWrite) {
      putAt(bb, idx, (buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit) ⇒ Future(mmapPut(buffer, pos, callback)))
    } else {
      throw new MmapWriteNotEnabled
    }
  def putRangeFAsync(bb: ByteBuffer, offsets: Array[Int])(implicit executionContext: ExecutionContext): PutResult[Future[GetResult]] =
    putRange(bb, offsets, (buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit) ⇒ Future(async.putX(buffer, pos, callback)).flatten)
  def putRangeFAsyncMmap(bb: ByteBuffer, offsets: Array[Int])(implicit executionContext: ExecutionContext): PutResult[Future[Int]] =
    if (mmapWrite) {
      putRange(bb, offsets, (buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit) ⇒ Future(mmapPut(buffer, pos, callback)))
    } else {
      throw new MmapWriteNotEnabled
    }
  def putRangeAtFAsync(bb: ByteBuffer, offsets: Array[Int], idx: Int)(implicit executionContext: ExecutionContext): PutResult[Future[GetResult]] =
    putRangeAt(bb, offsets, idx, (buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit) ⇒ Future(async.putX(buffer, pos, callback)).flatten)
  def putRangeAtFAsyncMmap(bb: ByteBuffer, offsets: Array[Int], idx: Int)(implicit executionContext: ExecutionContext): PutResult[Future[Int]] =
    if (mmapWrite) {
      putRangeAt(bb, offsets, idx, (buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit) ⇒ Future(mmapPut(buffer, pos, callback)))
    } else {
      throw new MmapWriteNotEnabled
    }

  def get(idx: Int): Future[ByteBuffer] = {
    if (idx < 0) {
      throw new NegativeSlotException(s"idx=$idx")
    }
    if (idx > readWatermark) {
      throw new ReadAboveWatermarkException(s"idx=$idx > readWatermark=$readWatermark")
    }
    val pos = if (idx == 0) SLOTS_LIMIT else slots.get(idx - 1)
    val len = slots.get(idx) - pos
    val bb = bufferPool.allocate(len)
    if (len > 0) {
      async.getX(bb, pos)
    } else {
      Future.successful(bb)
    }
  }

  def getRange(x: Int, y: Int): Future[ByteBuffer] = {
    if (x > readWatermark || y < 0) {
      return Future.successful(bufferPool.allocate(0))
    }
    val nx = 0.max(x)
    val ny = y.min(readWatermark)
    val pos = if (nx == 0) SLOTS_LIMIT else slots.get(nx - 1)
    val len = slots.get(ny) - pos
    val bb = bufferPool.allocate(len)
    if (len > 0) {
      async.getX(bb, pos)
    } else {
      Future.successful(bb)
    }
  }

  def getRangeIterator(x: Int, y: Int)(implicit executionContext: ExecutionContext): Future[Iterator[ByteBuffer]] = {
    if (x >= readWatermark || y < 0) {
      return Future.successful(Iterator.empty)
    }
    val nx = 0.max(x)
    val ny = y.min(readWatermark)
    Future.sequence(nx.to(ny).iterator.map(i ⇒ get(i)))
  }

  def shrink(newSize: Int): Unit = writeLock.synchronized {
    if (newSize < 0) {
      throw new NegativeSlotException(s"newSize=$newSize < 0")
    }
    if (newSize < currSlot.get) {
      if (queueSize > 0) {
        Thread.sleep(1000)
        val len = queueSize
        if (len > 0) {
          throw new IllegalStateException(s"Unable to shrink file with pending IO on store, writeQueue.size=$len")
        }
      }
      val maxSlot = currSlot.get
      currSlot.set(newSize)
      for (i ← newSize until maxSlot) slots.put(i, 0)
      currPos.set(if (newSize > 0) slots.get(newSize - 1) else SLOTS_LIMIT)
      readWatermark = newSize - 1
    }
  }

  def copyTo(toFile: File): FileRangeStore = {
    commitAll()
    toFile.delete()
    val toChannel = new RandomAccessFile(toFile, "rw").getChannel
    writeLock.synchronized(toChannel.transferFrom(this.channel.position(0), 0, this.channel.size()))
    toChannel.force(true)
    new FileRangeStore(toFile, totalSlots)
  }

  def print(limit: Int = 100): Unit = {
    println("*" * 30)
    println(s"currSlot=${currSlot.get}, currPos=${currPos.get}, SLOTS_LIMIT=$SLOTS_LIMIT, totalSlots=$totalSlots, readWatermark=$readWatermark")
    println(s"slots=${(0 until currSlot.get).map(slots.get).map(_ - SLOTS_LIMIT).mkString(", ")}")
    val sz = if (currSlot.get == 0) 0 else limit.min(slots.get(currSlot.get - 1) - SLOTS_LIMIT)
    val memory = bufferPool.allocate(sz)
    channel.getX(memory, SLOTS_LIMIT)
    println(s"memory=${memory.mkString(", ")}")
    var i = 0
    var sz1 = 0
    while (sz1 < limit && i < currSlot.get) {
      val bb = get(i).await()
      println(s"$i -> ${bb.mkString(", ")}")
      i += 1
      sz1 += bb.limit()
    }
    println("*" * 30)
  }
}
