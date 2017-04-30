package database

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.{AsynchronousFileChannel, CompletionHandler, FileChannel}
import java.nio.file.StandardOpenOption
import BufferUtil._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

// todo: cleanup after failed write
// todo: bulk load api
// todo: sort exception for use cases (should be different!!!)
object FileRangeStore {
  val STEP: Int = 4096
  val RESERVED_LIMIT: Int = 65536
  val MAX_POS: Int = Int.MaxValue
  val MAX_WRITE_QUEUE = 10

  class MMInt(mmap: MappedByteBuffer, addr: Int, initValue: Option[Int]) {
    private var cache: Option[Int] = None
    def get: Int = cache.getOrElse(this.synchronized {
      val value = mmap.getInt(addr)
      cache = Some(value)
      value
    })
    def set(value: Int): Unit = {
      this.synchronized(mmap.putInt(addr, value))
      cache = Some(value)
    }
    def +=(addition: Int): Int = this.synchronized {
      val value = get + addition
      mmap.putInt(addr, value)
      cache = Some(value)
      value
    }
    initValue.foreach(set)
  }

  class MMIntArray(mmap: MappedByteBuffer) {
    def set(idx: Int, value: Int): Unit = mmap.putInt(idx * 4, value)
    def get(idx: Int): Int = mmap.getInt(idx * 4)
  }

  case class AsyncResult(result: Int, buffer: ByteBuffer)

  implicit class RichAsynchronousFileChannel(async: AsynchronousFileChannel) {
    def put(buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit): Future[AsyncResult] = {
      val p = Promise[AsyncResult]()
      async.write(buffer, pos, buffer, new CompletionHandler[Integer, ByteBuffer]() {
        override def completed(result: Integer, attachment: ByteBuffer): Unit = {
          callback(true)
          p.success(AsyncResult(result.toInt, attachment))
        }
        override def failed(exc: Throwable, attachment: ByteBuffer): Unit = {
          callback(false)
          p.failure(exc)
        }
      })
      p.future
    }
    def get(buffer: ByteBuffer, pos: Int): Future[ByteBuffer] = {
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
    def put(buffer: ByteBuffer, pos: Int, callback: Boolean ⇒ Unit): Int = {
      val res = Try(channel.write(buffer, pos))
      callback(res.isSuccess)
      res.get
    }
    def get(buffer: ByteBuffer, pos: Int): ByteBuffer = {
      channel.read(buffer, pos)
      buffer
    }
  }

  class WriteQueueOverflowException(s: String) extends RuntimeException(s: String)

}

class FileRangeStore(val file: File, totalSlots: Int) extends RangeAsyncApi {
  import FileRangeStore._
  /*
       Small file structure:
       0 - 64K => reserved for control needs
       64k - 64k + 4 * slots => offsets
       64k + 4 * slots => data
  */

  val SLOTS_SIZE: Int = if (totalSlots % 1024 == 0) totalSlots * 4 else (totalSlots * 4 / STEP + 1) * STEP
  val SLOTS_LIMIT: Int = RESERVED_LIMIT + SLOTS_SIZE

  private val fileCreated: Boolean = !file.exists()
  val raf: RandomAccessFile = new RandomAccessFile(file, "rw")
  val channel: FileChannel = raf.getChannel
  val async: AsynchronousFileChannel = AsynchronousFileChannel.open(
    file.toPath,
    StandardOpenOption.READ,
    StandardOpenOption.WRITE,
    StandardOpenOption.CREATE
  )

  private val bufferPool = new BufferPool
  private val reserved_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, RESERVED_LIMIT)
  private val slots_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, RESERVED_LIMIT, SLOTS_SIZE)

  reserved_mmap.load()
  slots_mmap.load()

  def commitReserved(): Unit = reserved_mmap.force()
  def commitSlots(): Unit = slots_mmap.force()
  def commitAll(): Unit = async.force(false)

  private val currSlot = new MMInt(reserved_mmap, 0, if (fileCreated) Some(0) else None)
  private val currPos = new MMInt(reserved_mmap, 4, if (fileCreated) Some(SLOTS_LIMIT) else None)
  private val slots = new MMIntArray(slots_mmap)
  @volatile private var readWatermark = if (fileCreated) -1 else currSlot.get - 1

  private object writeLock
  private object readWatermarkLock

  def size: Int = currSlot.get

  // for text usage
  var maxQueueSize = 0
  private val writeQueue = scala.collection.mutable.SortedSet.empty[Int]
  private def encwq(slot: Int): Unit = readWatermarkLock.synchronized {
    if (writeQueue.size >= MAX_WRITE_QUEUE) {
      throw new WriteQueueOverflowException(s"writeQueue.size=${writeQueue.size} >= MAX_WRITE_QUEUE=$MAX_WRITE_QUEUE")
    }
    writeQueue.add(slot)
    if (maxQueueSize < writeQueue.size) {
      maxQueueSize = writeQueue.size
      //println(s"growing maxQueueSize=$maxQueueSize")
    }
  }
  // todo: ??? incorrect logic ???
  private def deqwq(slot: Int): Unit = readWatermarkLock.synchronized {
    writeQueue.remove(slot)
    val minSlot = writeQueue.headOption.getOrElse(slot + 1)
    readWatermark = minSlot - 1
  }
  def queueSize: Int = writeQueue.size
  // no sync method, we have to read readWatermark as it may be updated during the call
  def rwGap: Int = {
    val rwm = readWatermark
    val curr = currSlot.get
    curr - rwm - 1
  }
  def getReadWatermark: Int = readWatermark

  private def put[T](bb: ByteBuffer, putf: (ByteBuffer, Int, Boolean ⇒ Unit) ⇒ T): T = writeLock.synchronized {
    val len = bb.remaining()
    val slot = currSlot.get
    val pos = currPos.get
    val nextPos = pos.toLong + len
    if (nextPos > MAX_POS || currSlot.get >= totalSlots) {
      val msg = if (nextPos > MAX_POS) s"nextPos=$nextPos > MAX_POS=$MAX_POS" else s"currSlot.get=${currSlot.get} >= totalSlots=$totalSlots"
      throw new IndexOutOfBoundsException(msg)
    }
    encwq(slot)
    currPos.set(nextPos.toInt)
    slots.set(slot, nextPos.toInt)
    currSlot += 1
    val res = putf(bb, pos, _ ⇒ deqwq(slot))
    res
  }

  def putAsync(bb: ByteBuffer): Future[AsyncResult] = put(bb, async.put)
  def putSync(bb: ByteBuffer): Unit = put(bb, channel.put)
  def put(bb: ByteBuffer): Unit = putSync(bb)

  private def putAt[T](bb: ByteBuffer, idx: Int, putf: (ByteBuffer, Int, Boolean ⇒ Unit) ⇒ T): T = writeLock.synchronized {
    if (currSlot.get < idx && idx < totalSlots) {
      while (currSlot.get < idx) {
        slots.set(currSlot.get, currPos.get)
        currSlot += 1
      }
      put(bb, putf)
    } else {
      val msg = if (currSlot.get < idx) s"currSlot.get=${currSlot.get} < idx=$idx" else s"idx=$idx < totalSlots=$totalSlots"
      throw new IndexOutOfBoundsException(msg)
    }
  }
  def putAtAsync(bb: ByteBuffer, idx: Int): Future[AsyncResult] = putAt(bb, idx, async.put)
  def putAtSync(bb: ByteBuffer, idx: Int): Unit = putAt(bb, idx, channel.put)
  def putAt(bb: ByteBuffer, idx: Int): Unit = putAtSync(bb, idx)

  private def putRange[T](bb: ByteBuffer, offsets: Array[Int], putf: (ByteBuffer, Int, Boolean ⇒ Unit) ⇒ T): T = writeLock.synchronized {
    val len = bb.remaining()
    val pos = currPos.get
    val nextPos = currPos.get.toLong + len
    val maxSlot = currSlot.get - 1 + offsets.length
    if (nextPos > MAX_POS || maxSlot >= totalSlots) {
      val msg = if (nextPos > MAX_POS) s"nextPos=$nextPos > MAX_POS=$MAX_POS" else s"(currSlot.get=${currSlot.get} - 1 + offsets.length=${offsets.length})=maxSlot=$maxSlot >= totalSlots=$totalSlots"
      throw new IndexOutOfBoundsException(msg)
    }
    encwq(maxSlot)
    for (i ← offsets.indices) {
      slots.set(currSlot.get, currPos.get + offsets(i))
      currSlot += 1
    }
    currPos += len
    slots.set(currSlot.get, currPos.get)
    currSlot += 1
    val res = putf(bb, pos, _ ⇒ deqwq(maxSlot))
    res
  }

  def putRangeAsync(bb: ByteBuffer, offsets: Array[Int]): Future[AsyncResult] = putRange(bb, offsets, async.put)
  def putRangeSync(bb: ByteBuffer, offsets: Array[Int]): Unit = putRange(bb, offsets, channel.put)
  def putRange(bb: ByteBuffer, offsets: Array[Int]): Unit = putRangeSync(bb, offsets)

  def get(idx: Int): Future[ByteBuffer] = {
    if (idx > readWatermark || idx < 0) {
      val msg = if (idx > readWatermark) s"idx=$idx > readWatermark=$readWatermark" else s"idx=$idx < 0"
      throw new IndexOutOfBoundsException(msg)
    }
    val pos = if (idx == 0) SLOTS_LIMIT else slots.get(idx - 1)
    val len = slots.get(idx) - pos
    val bb = bufferPool.allocate(len)
    if (len > 0) {
      async.get(bb, pos)
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
      async.get(bb, pos)
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
    if (newSize < 0 || newSize > currSlot.get) {
      val msg = if (newSize < 0) s"newSize=$newSize < 0" else s"newSize=$newSize > currSlot.get=${currSlot.get}"
      throw new IndexOutOfBoundsException(msg)
    }
    if (writeQueue.nonEmpty) {
      Thread.sleep(1000)
      val len = writeQueue.size
      if (len > 0) {
        throw new IllegalStateException(s"Unable to shrink file with pending IO on store, writeQueue.size=$len")
      }
    }
    val maxSlot = currSlot.get
    currSlot.set(newSize)
    for (i ← newSize until maxSlot) slots.set(i, 0)
    currPos.set(if (newSize > 0) slots.get(newSize - 1) else SLOTS_LIMIT)
    readWatermark = newSize - 1
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
    val sz = limit.min(slots.get(currSlot.get - 1) - SLOTS_LIMIT)
    val memory = bufferPool.allocate(sz)
    channel.get(memory, SLOTS_LIMIT)
    println(s"memory=${memory.mkString(", ")}")
    var i = 0
    var sz1 = 0
    while (sz1 < limit && i < currSlot.get) {
      val bb = get(i).await
      i += 1
      sz1 += bb.limit()
      println(s"i -> ${bb.mkString(", ")}")
    }
    println("*" * 30)
  }
}
