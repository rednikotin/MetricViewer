package database

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util._
import FileRangeStore._
import database.FileRangeStoreWithSortingBuffer._

object FileRangeStoreWithSortingBuffer {
  trait SpaceManager {
    def allocated(pos: Int, len: Int): Unit
    def allocate(len: Int): Int
    def release(pos: Int, len: Int): Unit
    def clear(): Unit
    def getFreeSpace: Iterator[(Int, Int)]
  }

  class NoSpaceLeftException(msg: String) extends RuntimeException(msg)
  class FragmentationException(msg: String) extends RuntimeException(msg)

  def intervalSM(size: Int): SpaceManager = {
    new SpaceManager {
      private val intervals = new Intervals.IntervalSet(0, size)
      def allocated(pos: Int, len: Int): Unit = intervals.allocated(pos, len)
      def allocate(len: Int): Int = try {
        intervals.allocate(len)._1
      } catch {
        case ex: Intervals.AllocationFailedException ⇒
          throw new NoSpaceLeftException(ex.msg)
        case ex: Intervals.FragmentationException ⇒
          throw new FragmentationException(ex.msg)
      }
      def release(pos: Int, len: Int): Unit = intervals.release(pos, len)
      def clear(): Unit = intervals.clear()
      def getFreeSpace: Iterator[(Int, Int)] = intervals.getIntervals.iterator
    }
  }

  def intervalRR(size: Int): SpaceManager = {
    new SpaceManager {
      private var curPos = 0
      def release(pos: Int, len: Int): Unit = {}
      def clear(): Unit = curPos = 0
      def getFreeSpace: Iterator[(Int, Int)] = Seq((curPos, size - curPos)).iterator
      def allocate(len: Int): Int = if (curPos + len <= size) {
        val res = curPos
        curPos += len
        res
      } else throw new NoSpaceLeftException("No Space Left!")
      def allocated(pos: Int, len: Int): Unit = curPos = curPos.max(pos + len)
    }
  }

  sealed trait SpaceManagerType
  object SM extends SpaceManagerType
  object RR extends SpaceManagerType

  case class CountStats(
      flushNoSpaceCount:    Int,
      flushNoSlotCount:     Int,
      defragmentationCount: Int,
      flushPrefixCount:     Int,
      ignoreCount:          Int,
      tooBig:               Int
  ) {
    override def toString: String =
      s"{flushNoSpaceCount=$flushNoSpaceCount, " +
        s"flushNoSlotCount=$flushNoSlotCount, " +
        s"defragmentationCount=$defragmentationCount, " +
        s"flushPrefixCount=$flushPrefixCount, " +
        s"ignoreCount=$ignoreCount, " +
        s"tooBig=$tooBig}"
  }
}

class FileRangeStoreWithSortingBuffer(file: File, totalSlots: Int, withCrean: Boolean = false, spaceManagerType: SpaceManagerType = SM) extends FileRangeStore(file, totalSlots, withCrean) {

  /*
       Small file structure:
       0 - 1m => reserved for control needs:
       0,1
       320k => sb-slot to slot mapping
       384k => sb-slot len
       448k => sb-slot positions
       512k => sb-data
       1m => offsets
       > 1m + 4 * slots => data
  */

  @volatile private var flushNoSpaceCount = 0
  @volatile private var flushNoSlotCount = 0
  @volatile private var defragmentationCount = 0
  @volatile private var flushPrefixCount = 0
  @volatile private var ignoreCount = 0
  @volatile private var tooBig = 0
  def resetCounters(): Unit = {
    flushNoSpaceCount = 0
    flushNoSlotCount = 0
    defragmentationCount = 0
    flushPrefixCount = 0
    ignoreCount = 0
    tooBig = 0
  }
  def getCountStats: CountStats = CountStats(flushNoSpaceCount, flushNoSlotCount, defragmentationCount, flushPrefixCount, ignoreCount, tooBig)

  private val sb_slots_map_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, SORTING_BUFFER_FIRST_SLOT_MAP, SORTING_BUFFER_SLOTS_SIZE)
  private val sb_slots_len_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, SORTING_BUFFER_FIRST_SLOT_LEN, SORTING_BUFFER_SLOTS_SIZE)
  private val sb_slots_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, SORTING_BUFFER_FIRST_SLOT, SORTING_BUFFER_SLOTS_SIZE)
  private val sb_data_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, SORTING_BUFFER_FIRST_SLOT + SORTING_BUFFER_SLOTS_SIZE, SORTING_BUFFER_DATA_SIZE)
  def commitSortingBuffer(): Unit = {
    sb_slots_map_mmap.force()
    sb_slots_len_mmap.force()
    sb_slots_mmap.force()
    sb_data_mmap.force()
  }

  // sb_slots - contains (pos + 1) -- we have to +-1 with operation,
  // this is needed to prevent mess with empty fiels with zeros. So zero mean empty slot rahter than -1
  private val sb_slots = sb_slots_mmap.asIntBuffer()
  private val sb_slots_len = sb_slots_len_mmap.asIntBuffer()
  private val sb_slots_map = sb_slots_map_mmap.asIntBuffer()

  // /*private*/ val sbMap: mutable.TreeMap[Int, Int] = collection.mutable.TreeMap.empty[Int, Int]
  /*private*/ val sbMap: JavaTreeMap[Int, Int] = new JavaTreeMap[Int, Int]()
  /*private*/ val sb_free_slot: mutable.ArrayStack[Int] = mutable.ArrayStack[Int]()
  /*private*/ val sb_free_space: SpaceManager = spaceManagerType match {
    case SM ⇒ intervalSM(SORTING_BUFFER_DATA_SIZE)
    case RR ⇒ intervalRR(SORTING_BUFFER_DATA_SIZE)
  }

  if (!initializationRequired) {
    for (sbslot ← 0 until SORTING_BUFFER_TOTAL_SLOTS) {
      val pos = sb_slots.get(sbslot) - 1
      if (pos != -1) {
        val slot = sb_slots_map.get(sbslot)
        val len = sb_slots_len.get(sbslot)
        sb_free_space.allocated(pos, len)
        sbMap += slot → sbslot
      } else {
        sb_free_slot.push(sbslot)
      }
    }
  } else {
    resetSlots()
  }

  private def resetSlots(): Unit = {
    for (sbslot ← 0 until SORTING_BUFFER_TOTAL_SLOTS) sb_slots.put(sbslot, 0)
    sb_free_slot.clear()
    sb_free_slot ++= (0 until SORTING_BUFFER_TOTAL_SLOTS)
    sb_free_space.clear()
    sbMap.clear()
  }

  private def getsb(sbslot: Int): ByteBuffer = {
    val pos = sb_slots.get(sbslot) - 1
    val len = sb_slots_len.get(sbslot)
    val array = new Array[Byte](len)
    sb_data_mmap.position(pos)
    sb_data_mmap.get(array)
    ByteBuffer.wrap(array)
  }

  private def defragmentation(): Unit = {
    val data = sbMap.map { case (slot, sbslot) ⇒ (getsb(sbslot), slot) }
    resetSlots()
    data.foreach { case (buffer, slot) ⇒ sb_put(buffer, slot) }
    defragmentationCount += 1
  }

  private def putRangeAtForce(buffer: ByteBuffer, offsets: Array[Int], slot: Int): Unit = {
    var retries = 0
    var inserted = false
    while (!inserted) {
      try {
        putRangeAtSync(buffer, offsets, slot)
        inserted = true
      } catch {
        case ex: SlotAlreadyUsedException ⇒
          logger.info(s"got SlotAlreadyUsedException(${ex.msg}) while flushing sorting buffer slot=$slot")
        case ex: WriteQueueOverflowException ⇒
          retries += 1
          if (retries > 10) {
            throw new WriteQueueOverflowException(s"Unable to flush buffer, getting exception > 10 times, ${ex.msg}")
          }
          Thread.sleep(200)
      }
    }
  }

  private val flushingBuffer = ByteBuffer.allocateDirect(SORTING_BUFFER_DATA_SIZE + SORTING_BUFFER_FLUSH_AUX)

  private def flushPrefix(): Unit = {
    val prefix = ArrayBuffer.empty[(Int, Int)]
    var isPrefix = true
    val iter = sbMap.iterator
    val cs = currSlot.get
    var sidx = 0
    var offset = 0
    val offsets = mutable.ArrayBuffer.empty[Int]
    while (isPrefix && iter.hasNext) {
      val (slot, sbslot) = iter.next()
      if (slot == cs + sidx) {
        val pos = sb_slots.get(sbslot) - 1
        val len = sb_slots_len.get(sbslot)
        var idx = 0
        while (idx < len) {
          flushingBuffer.put(sb_data_mmap.get(pos + idx))
          idx += 1
        }
        offset += len
        offsets += offset
        sidx += 1
        prefix += ((slot, sbslot))
      } else {
        isPrefix = false
      }
    }
    flushingBuffer.flip()
    putRangeAtForce(flushingBuffer, offsets.toArray, cs)
    flushingBuffer.clear()
    prefix.foreach {
      case (slot, sbslot) ⇒
        val pos = sb_slots.get(sbslot) - 1
        val len = sb_slots_len.get(sbslot)
        sbMap -= slot
        sb_free_space.release(pos, len)
        sb_slots.put(sbslot, 0)
        sb_free_slot.push(sbslot)
    }
    flushPrefixCount += 1
  }

  private def flush(buffer: ByteBuffer, slot: Int): Future[Any] = {
    val isAfter = sbMap.isEmpty || slot > sbMap.lastKey
    val bufferLen = buffer.remaining()
    val sbLen = sbMap.values.map(sb_slots_len.get).sum
    val isTooBig = bufferLen > flushingBuffer.capacity() - sbLen
    val isEnormous = bufferLen > SORTING_BUFFER_DATA_SIZE
    if (isTooBig) tooBig += 1
    if (!isAfter) sbMap += slot → -1
    val iter = sbMap.iterator
    var cs = currSlot.get
    var sidx = 0
    var offset = 0
    val offsets = mutable.ArrayBuffer.empty[Int]
    while (iter.hasNext) {
      val (slot, sbslot) = iter.next()
      while (cs + sidx < slot) {
        offsets += offset
        sidx += 1
      }
      if (sbslot >= 0) {
        val pos = sb_slots.get(sbslot) - 1
        val len = sb_slots_len.get(sbslot)
        var idx = 0
        while (idx < len) {
          flushingBuffer.put(sb_data_mmap.get(pos + idx))
          idx += 1
        }
        offset += len
      } else {
        if (isTooBig) {
          flushingBuffer.flip()
          if (flushingBuffer.hasRemaining) {
            putRangeAtForce(flushingBuffer, offsets.toArray, cs)
          }
          putAtSync(buffer, slot)
          flushingBuffer.clear()
          offsets.clear()
          offset = 0
          cs = currSlot.get
          sidx = 0
        } else {
          flushingBuffer.put(buffer)
          offset += bufferLen
        }
      }
      offsets += offset
      sidx += 1
    }
    flushingBuffer.flip()
    if (flushingBuffer.hasRemaining) {
      putRangeAtForce(flushingBuffer, offsets.toArray, cs)
    }
    flushingBuffer.clear()
    resetSlots()
    if (isAfter) {
      if (isEnormous) {
        putAtAsync(buffer, slot).result
      } else {
        putAtViaSortingBuffer(buffer, slot)
      }
    } else {
      Future.successful()
    }
  }

  /*  private def putAtForce(buffer: ByteBuffer, slot: Int): Unit = {
    var retries = 0
    var inserted = false
    while (!inserted) {
      try {
        putAtSync(buffer, slot)
        inserted = true
      } catch {
        case ex: SlotAlreadyUsedException ⇒
          logger.info(s"got SlotAlreadyUsedException(${ex.msg}) while flushing sorting buffer slot=$slot")
        case ex: WriteQueueOverflowException ⇒
          retries += 1
          if (retries > 10) {
            throw new WriteQueueOverflowException(s"Unable to flush buffer, getting exception > 10 times, ${ex.msg}")
          }
          Thread.sleep(10)
      }
    }
  }

  private def flushPrefix(): Unit = {
    val prefix = ArrayBuffer.empty[(Int, Int)]
    var isPrefix = true
    val iter = sbMap.iterator
    while (isPrefix && iter.hasNext) {
      val (slot, sbslot) = iter.next()
      if (slot == currSlot.get) {
        val buffer = getsb(sbslot)
        putAtForce(buffer, slot)
        prefix += ((slot, sbslot))
      } else {
        isPrefix = false
      }
    }
    prefix.foreach {
      case (slot, sbslot) ⇒
        val pos = sb_slots.get(sbslot)
        val len = sb_slots_len.get(sbslot)
        sbMap -= slot
        sb_free_space.release(pos, len)
        sb_slots.put(sbslot, -1)
    }
  }

  private def flush(): Unit = {
    for ((slot, sbslot) ← sbMap) putAtForce(getsb(sbslot), slot)
    resetSlots()
  }*/

  private def sb_put(buffer: ByteBuffer, slot: Int): Future[Any] = {
    if (sb_free_slot.isEmpty) {
      flushNoSlotCount += 1
      flush(buffer, slot)
    } else {
      val len = buffer.remaining()
      Try(sb_free_space.allocate(len)).recoverWith {
        case _: FragmentationException ⇒
          defragmentation()
          Try(sb_free_space.allocate(len))
      } match {
        case Success(pos) ⇒
          val sbslot = sb_free_slot.pop()
          sbMap += slot → sbslot
          sb_slots.put(sbslot, pos + 1)
          sb_slots_len.put(sbslot, len)
          sb_slots_map.put(sbslot, slot)
          sb_data_mmap.position(pos)
          sb_data_mmap.put(buffer)
          Future.successful()
        case Failure(ex) ⇒ ex match {
          case _: NoSpaceLeftException ⇒
            flushNoSpaceCount += 1
            flush(buffer, slot)
          case ex: Throwable ⇒
            throw ex
        }
      }
    }
  }

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
      if (sbMap.headOption.map(_._1).contains(currSlot.get)) {
        flushPrefix()
      }
      res.result
    } else {
      sb_put(bb, idx)
    }
  }

  def putAtViaSortingBufferSilent(bb: ByteBuffer, idx: Int): Future[Any] = writeLock.synchronized {
    if (idx < currSlot.get) {
      ignoreCount += 1
      Future.successful()
    } else {
      putAtViaSortingBuffer(bb, idx)
    }
  }

  override def shrink(newSize: Int): Unit = writeLock.synchronized {
    resetSlots()
    super.shrink(newSize)
  }

  override def print(limit: Int = 100) {
    super.print(limit)
    val freeSpaceMsg = sb_free_space.getFreeSpace.map(interval ⇒ s"[${interval._1}, +${interval._2}]").mkString(", ")
    println(s"sb_free_space=$freeSpaceMsg")
    println(s"sbMap=${sbMap.toList.map(x ⇒ s"${x._1}->${x._2}").mkString(",")}")
    println("*" * 30)
  }

  override def copyTo(toFile: File): FileRangeStore = {
    commitAll()
    toFile.delete()
    val toChannel = new RandomAccessFile(toFile, "rw").getChannel
    writeLock.synchronized(toChannel.transferFrom(this.channel.position(0), 0, this.channel.size()))
    toChannel.force(true)
    new FileRangeStoreWithSortingBuffer(toFile, totalSlots)
  }

}