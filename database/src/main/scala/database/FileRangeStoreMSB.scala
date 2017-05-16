package database

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.{AsynchronousFileChannel, FileChannel}
import java.nio.file.StandardOpenOption
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import FileRangeStore._
import database.FileRangeStoreMSB._

object FileRangeStoreMSB {

  case class Buffered(buffer: ByteBuffer) {
    val timestamp: Long = System.currentTimeMillis()
  }

  case class CountStats(
      flushObsoletePrefixCount: Int,
      flushPrefixCount:         Int,
      ignoreCount:              Int,
      switched:                 Int
  ) {
    override def toString: String =
      s"{flushObsoletePrefixCount=$flushObsoletePrefixCount, " +
        s"flushPrefixCount=$flushPrefixCount, " +
        s"ignoreCount=$ignoreCount, " +
        s"switched=$switched}"
  }
}

class FileRangeStoreMSB(file: File, totalSlots: Int, keepTime: Int, withClean: Boolean = false) extends FileRangeStore(file, totalSlots, withClean) {

  @volatile private var flushObsoletePrefixCount = 0
  @volatile private var flushPrefixCount = 0
  @volatile private var ignoreCount = 0
  def resetCounters(): Unit = {
    flushObsoletePrefixCount = 0
    flushPrefixCount = 0
    ignoreCount = 0
    sb.resetStats()
  }
  def getCountStats: CountStats = CountStats(flushObsoletePrefixCount, flushPrefixCount, ignoreCount, sb.getSwitched)

  val sb: SortedIntMapX[Buffered] = new SortedIntMapX[Buffered](null)
  //val sb: SortedIntMap[Buffered] = new SortedIntMapJ[Buffered]()

  private def putRangeAtForce(buffer: ByteBuffer, offsets: ArrayBuffer[Int], slot: Int): Unit = {
    var retries = 0
    var inserted = false
    while (!inserted) {
      try {
        putRangeAtSync(buffer, offsets, slot)
        inserted = true
      } catch {
        case ex: SlotAlreadyUsedException ⇒
          logger.info(s"got SlotAlreadyUsedException(${ex.msg}) while flushing sorting buffer slot=$slot")
          inserted = true
        case ex: WriteQueueOverflowException ⇒
          retries += 1
          if (retries > 10) {
            throw new WriteQueueOverflowException(s"Unable to flush buffer, getting exception > 10 times, ${ex.msg}")
          }
          Thread.sleep(200)
      }
    }
  }

  private val flushingBuffer = ByteBuffer.allocateDirect(SORTING_BUFFER_FLUSH_SIZE)

  private def flushPrefix(): Unit = {
    val prefix = ArrayBuffer.empty[Int]
    var isPrefix = true
    val iter = sb.iterator
    var cs = currSlot.get
    var sidx = 0
    var offset = 0
    val offsets = mutable.ArrayBuffer.empty[Int]
    while (isPrefix && iter.hasNext) {
      val (slot, buffered) = iter.next()
      if (slot == cs + sidx) {
        val len = buffered.buffer.remaining()
        if (flushingBuffer.remaining() < len) {
          flushingBuffer.flip()
          if (offsets.nonEmpty) {
            putRangeAtForce(flushingBuffer, offsets, cs)
          }
          flushingBuffer.clear()
          offset = 0
          offsets.clear()
          cs = currSlot.get
          sidx = 0
        }
        if (flushingBuffer.remaining() < len) {
          offsets += len
          putRangeAtForce(buffered.buffer, offsets, cs)
          offsets.clear()
          cs = currSlot.get
          sidx = 0
        } else {
          flushingBuffer.put(buffered.buffer)
          offset += len
          offsets += offset
          sidx += 1
        }
        prefix += slot
      } else {
        isPrefix = false
      }
    }
    flushingBuffer.flip()
    if (flushingBuffer.hasRemaining) {
      putRangeAtForce(flushingBuffer, offsets, cs)
      flushingBuffer.clear()
    }
    prefix.foreach(sb.-=)
    flushPrefixCount += 1
  }

  private def sb_put(buffer: ByteBuffer, slot: Int): Unit = {
    sb += slot → Buffered(buffer)
    flushObsolete()
  }

  def flushObsolete(): Unit = {
    val obsoleteTime = System.currentTimeMillis() - keepTime
    if (sb.headOption.exists(x ⇒ x._2.timestamp < obsoleteTime)) {
      val prefix = ArrayBuffer.empty[Int]
      var isPrefix = true
      val iter = sb.iterator
      var cs = currSlot.get
      var sidx = 0
      var offset = 0
      val offsets = mutable.ArrayBuffer.empty[Int]
      while (isPrefix && iter.hasNext) {
        val (slot, buffered) = iter.next()
        if (buffered.timestamp < obsoleteTime) {
          while (slot > cs + sidx) {
            offsets += offset
            sidx += 1
          }
          val len = buffered.buffer.remaining()
          if (flushingBuffer.remaining() < len) {
            flushingBuffer.flip()
            if (offsets.nonEmpty) {
              putRangeAtForce(flushingBuffer, offsets, cs)
            }
            flushingBuffer.clear()
            offset = 0
            offsets.clear()
            cs = currSlot.get
            sidx = 0
          }
          if (flushingBuffer.remaining() < len) {
            offsets += len
            putRangeAtForce(buffered.buffer, offsets, cs)
            offsets.clear()
            cs = currSlot.get
            sidx = 0
          } else {
            flushingBuffer.put(buffered.buffer)
            offset += len
            offsets += offset
            sidx += 1
          }
          prefix += slot
        } else {
          isPrefix = false
        }
      }
      flushingBuffer.flip()
      if (offsets.nonEmpty) {
        putRangeAtForce(flushingBuffer, offsets, cs)
        flushingBuffer.clear()
      }
      prefix.foreach(sb.-=)
      flushObsoletePrefixCount += 1
    }
  }

  // todo: prepare types of results (eith if scheduled to write or put in buffer or late result in trash segment?)
  def putAtViaSortingBuffer(bb: ByteBuffer, idx: Int): Future[Any] = writeLock.synchronized {
    if (idx >= totalSlots) {
      throw new NoAvailableSlotsException(s"idx=$idx >= totalSlots=$totalSlots")
    }
    if (idx < 0) {
      throw new NegativeSlotException(s"idx=$idx")
    }
    if (idx < currSlot.get) {
      throw new SlotAlreadyUsedException(s"idx=$idx < currSlot.get=${currSlot.get}")
    }
    if (idx == currSlot.get) {
      val res = putAsync(bb)
      if (sb.headOption.map(_._1).contains(currSlot.get)) {
        flushPrefix()
      }
      res.result
    } else {
      sb_put(bb, idx)
      Future.successful()
    }
  }

  def putAtViaSortingBufferSilent(bb: ByteBuffer, idx: Int): Future[Any] = writeLock.synchronized {
    if (idx < currSlot.get) {
      ignoreCount += 1
      moveTrash(bb, idx)
    } else {
      putAtViaSortingBuffer(bb, idx)
    }
  }

  override def shrink(newSize: Int): Unit = writeLock.synchronized {
    sb.clear()
    super.shrink(newSize)
  }

  override def print(limit: Int = 100) {
    super.print(limit)
    println(s"sbMap=${sb.toList.map(x ⇒ s"${x._1}->${x._2}").mkString(",")}")
    println("*" * 30)
  }

  override def copyTo(toFile: File): FileRangeStoreMSB = {
    commitAll()
    toFile.delete()
    val toChannel = new RandomAccessFile(toFile, "rw").getChannel
    writeLock.synchronized(toChannel.transferFrom(this.channel.position(0), 0, this.channel.size()))
    toChannel.force(true)
    val res = new FileRangeStoreMSB(toFile, totalSlots, keepTime)
    sb.iterator.foreach(x ⇒ res.sb += x)
    res
  }

  private var trash_enabled = false
  private var trash_channel: FileChannel = _
  private var trash_async: AsynchronousFileChannel = _
  private var trash_mmap: MappedByteBuffer = _
  private var trashPos: MMInt = _
  def enableTrash(file: File): Unit = {
    trash_enabled = true
    val created = !file.exists()
    val raf: RandomAccessFile = new RandomAccessFile(file, "rw")
    trash_channel = raf.getChannel
    trash_async = AsynchronousFileChannel.open(
      file.toPath,
      StandardOpenOption.READ,
      StandardOpenOption.WRITE,
      StandardOpenOption.CREATE
    )
    trash_mmap = trash_channel.map(FileChannel.MapMode.READ_WRITE, 0, TRASH_RESERVED)
    trashPos = new MMInt(trash_mmap, 0, if (created) Some(TRASH_RESERVED) else None)
  }
  def moveTrash(bb: ByteBuffer, slot: Int): Future[Any] = if (trash_enabled) {
    val pos = trashPos.get
    val len = 8 + bb.remaining()
    trashPos += len
    val withMeta = bufferPool.allocate(len)
    withMeta.putInt(len)
    withMeta.putInt(slot)
    withMeta.put(bb)
    withMeta.flip()
    trash_async.putX(withMeta, pos, _ ⇒ {})
  } else {
    Future.successful()
  }
  def readTrashMeta: Iterator[TrashMeta] = new Iterator[TrashMeta] {
    private var pos = TRASH_RESERVED
    private val bb = ByteBuffer.allocate(8)
    def hasNext(): Boolean = pos < trashPos.get
    def next(): TrashMeta = {
      bb.clear()
      trash_channel.read(bb, pos)
      bb.flip()
      val id = bb.asIntBuffer()
      // TrashMeta(idx: Int, bufferLen: Int, pos: Int)
      val len = id.get(0)
      val res = TrashMeta(id.get(1), len - 8, pos + 8)
      if (len < 8) pos = Int.MaxValue else pos += len
      res
    }
  }
  def readTrashBufferAsync(meta: TrashMeta): Future[ByteBuffer] = {
    val bb = bufferPool.allocate(meta.bufferLen)
    async.getX(bb, meta.pos)
  }
  def readTrashBufferSync(meta: TrashMeta): ByteBuffer = {
    val bb = bufferPool.allocate(meta.bufferLen)
    trash_channel.getX(bb, meta.pos)
  }
  // return mutable collection, have to copy to immutable list before return
  def bufferedSlots: Iterable[Int] = sb.keys.toList
}