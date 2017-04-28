package database

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.{AsynchronousFileChannel, CompletionHandler, FileChannel}
import java.nio.file.{Paths, StandardOpenOption}
import scala.concurrent.duration._
import BufferUtil._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

class FileRangeStore(val file: File, totalSlots: Int) extends RangeAsyncApi {
  /*
       Small file structure:
       0 - 64K => reserved for control needs
       64k - 64k + 4 * slots => offsets
       64k + 4 * slots => data

       Big file structure: todo?
       0 - 64K => reserved for control needs
       64k - 64k + 8 * slots => offsets
       64k + 8 * slots => data
  */

  val STEP: Int = 4096
  val RESERVED_LIMIT: Int = 65536
  val SLOTS_SIZE: Int = if (totalSlots % 1024 == 0) totalSlots * 4 else (totalSlots * 4 / STEP + 1) * STEP
  val SLOTS_LIMIT: Int = RESERVED_LIMIT + SLOTS_SIZE
  val MAX_POS: Int = Int.MaxValue

  var fileCreated: Boolean = !file.exists()
  val raf: RandomAccessFile = new RandomAccessFile(file, "rw")
  val channel: FileChannel = raf.getChannel
  val async: AsynchronousFileChannel = AsynchronousFileChannel.open(
    file.toPath,
    StandardOpenOption.READ,
    StandardOpenOption.WRITE,
    StandardOpenOption.CREATE
  )

  val bufferPool = new BufferPool
  val reserved_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, RESERVED_LIMIT)
  val slots_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, RESERVED_LIMIT, SLOTS_SIZE)

  reserved_mmap.load()
  slots_mmap.load()

  def commitReserved(): Unit = reserved_mmap.force()
  def commitSlots(): Unit = slots_mmap.force()
  def commitAll(): Unit = async.force(false)

  class MMInt(mmap: MappedByteBuffer, addr: Int, initValue: Int) {
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
    if (fileCreated) set(initValue)
  }

  // concurrency is not strict, but it should be Ok: we only appending data here, so we have be careful about currSlot only
  class MMIntArray(mmap: MappedByteBuffer) {
    def set(idx: Int, value: Int): Unit = mmap.putInt(idx * 4, value)
    def get(idx: Int): Int = mmap.getInt(idx * 4)
  }

  // todo: buffers pool
  // todo: variable to show "safe read" < curr
  // todo: Put at specific location AFTER currentSlot -- all internal slots pointed to prev value to make empty-content

  case class AsyncResult(result: Int, buffer: ByteBuffer)
  def put0(buffer: ByteBuffer, pos: Int): Unit = async.write(buffer, pos)
  def put1(buffer: ByteBuffer, pos: Int): Future[AsyncResult] = {
    val p = Promise[AsyncResult]()
    async.write(buffer, pos, buffer, new CompletionHandler[Integer, ByteBuffer]() {
      override def completed(result: Integer, attachment: ByteBuffer): Unit = p.success(AsyncResult(result.toInt, attachment))
      override def failed(exc: Throwable, attachment: ByteBuffer): Unit = p.failure(exc)
    })
    p.future
  }
  def get1(buffer: ByteBuffer, pos: Int): Future[ByteBuffer] = {
    val p = Promise[ByteBuffer]()
    async.read(buffer, pos, buffer, new CompletionHandler[Integer, ByteBuffer]() {
      override def completed(result: Integer, attachment: ByteBuffer): Unit = p.success(attachment)
      override def failed(exc: Throwable, attachment: ByteBuffer): Unit = p.failure(exc)
    })
    p.future
  }
  def get2(buffer: ByteBuffer, pos: Int): Future[AsyncResult] = {
    val p = Promise[AsyncResult]()
    async.read(buffer, pos, buffer, new CompletionHandler[Integer, ByteBuffer]() {
      override def completed(result: Integer, attachment: ByteBuffer): Unit = p.success(AsyncResult(result.toInt, attachment))
      override def failed(exc: Throwable, attachment: ByteBuffer): Unit = p.failure(exc)
    })
    p.future
  }

  private val currSlot = new MMInt(reserved_mmap, 0, 0)
  private val currPos = new MMInt(reserved_mmap, 4, SLOTS_LIMIT)
  // slot.get(n) - start pos of next
  private val slots = new MMIntArray(slots_mmap)

  private object writeLock

  def size: Int = currSlot.get

  def put(bb: ByteBuffer): Unit = writeLock.synchronized {
    val len = bb.remaining()
    put0(bb, currPos.get)
    currPos += len
    slots.set(currSlot.get, currPos.get)
    currSlot += 1
  }

  def putAt(bb: ByteBuffer, idx: Int): Unit = ???

  def get(x: Int): Future[ByteBuffer] = {
    val pos = if (x == 0) SLOTS_LIMIT else slots.get(x - 1)
    val len = slots.get(x) - pos
    val bb = bufferPool.allocate(len)
    if (len > 0) {
      get1(bb, pos)
    } else {
      Future.successful(bb)
    }
  }

  def putRange(bb: ByteBuffer, offsets: Array[Int]): Unit = {
    val len = bb.remaining()
    put0(bb, currPos.get)
    for (i ← offsets.indices) {
      slots.set(currSlot.get, currPos.get + offsets(i))
      currSlot += 1
    }
    currPos += len
    slots.set(currSlot.get, currPos.get)
    currSlot += 1
  }

  def getRange(x: Int, y: Int): Future[ByteBuffer] = {
    if (x >= currSlot.get || y < 0) {
      return Future.successful(bufferPool.allocate(0))
    }
    val nx = 0.max(x)
    val ny = y.min(currSlot.get - 1)
    val pos = if (nx == 0) SLOTS_LIMIT else slots.get(nx - 1)
    val len = slots.get(ny) - pos
    val bb = bufferPool.allocate(len)
    if (len > 0) {
      get1(bb, pos)
    } else {
      Future.successful(bb)
    }
  }

  def getRangeIterator(x: Int, y: Int)(implicit executionContext: ExecutionContext): Future[Iterator[ByteBuffer]] = {
    if (x >= currSlot.get || y < 0) {
      return Future.successful(Iterator.empty)
    }
    val nx = 0.max(x)
    val ny = y.min(currSlot.get - 1)
    Future.sequence(nx.to(ny).iterator.map(i ⇒ get(i)))
  }

  def print(limit: Int = 100): Unit = {
    println("*" * 30)
    println(s"currSlot=${currSlot.get}, currPos=${currPos.get}, SLOTS_LIMIT=$SLOTS_LIMIT, totalSlots=$totalSlots")
    println(s"slots=${(0 until currSlot.get).map(slots.get).map(_ - SLOTS_LIMIT).mkString(", ")}")
    val sz = limit.min(slots.get(currSlot.get - 1) - SLOTS_LIMIT)
    val memory = bufferPool.allocate(sz)
    Await.result(get1(memory, SLOTS_LIMIT), 30 seconds)
    println(s"memory=${memory.mkString(", ")}")
    var i = 0
    var sz1 = 0
    while (sz1 < limit && i < currSlot.get) {
      val bb = Await.result(get(i), 30 seconds)
      i += 1
      sz1 += bb.limit()
      println(s"i -> ${bb.mkString(", ")}")
    }
    println("*" * 30)
  }
}
