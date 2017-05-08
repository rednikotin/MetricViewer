package database

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util._

object FileRangeStoreWithSortingBufferV2 {
  import FileRangeStore._
  final val SORTING_BUFFER_DATA_SIZE: Int = 32768
  final val SORTING_BUFFER_SLOTS_SIZE: Int = 4096
  final val SORTING_BUFFER_TOTAL_SLOTS: Int = SORTING_BUFFER_SLOTS_SIZE / 4
  final val SORTING_BUFFER_FIRST_SLOT: Int = RESERVED_LIMIT - SORTING_BUFFER_DATA_SIZE - SORTING_BUFFER_SLOTS_SIZE
  final val SORTING_BUFFER_FIRST_SLOT_LEN: Int = SORTING_BUFFER_FIRST_SLOT - SORTING_BUFFER_SLOTS_SIZE
  final val SORTING_BUFFER_FIRST_SLOT_MAP: Int = SORTING_BUFFER_FIRST_SLOT_LEN - SORTING_BUFFER_SLOTS_SIZE

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
        intervals.allocate(len).left
      } catch {
        case ex: Intervals.AllocationFailedException ⇒
          throw new NoSpaceLeftException(ex.msg)
        case ex: Intervals.FragmentationException ⇒
          throw new FragmentationException(ex.msg)
      }
      def release(pos: Int, len: Int): Unit = intervals.release(pos, len)
      def clear(): Unit = intervals.clear()
      def getFreeSpace: Iterator[(Int, Int)] = intervals.getIntervals.iterator.map(i ⇒ (i.left, i.length))
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
}

class FileRangeStoreWithSortingBufferV2(file: File, totalSlots: Int) extends FileRangeStore(file, totalSlots) {
  import FileRangeStore._
  import FileRangeStoreWithSortingBufferV2._

  /*
       Small file structure:
       0 - 64K => reserved for control needs:
       0,1
       20k - 24k => sb-slot to slot mapping
       24k - 28k => sb-slot len
       28k - 32k => sb-slot positions
       32k - 64k => sb-data
       64k - 64k + 4 * slots => offsets
       64k + 4 * slots => data
  */

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

  private val sb_slots = sb_slots_mmap.asIntBuffer()
  private val sb_slots_len = sb_slots_len_mmap.asIntBuffer()
  private val sb_slots_map = sb_slots_map_mmap.asIntBuffer()

  private val sbMap = collection.mutable.SortedMap.empty[Int, Int]
  private val sb_free_slot = mutable.ArrayStack[Int]()
  private val sb_free_space: SpaceManager =
    intervalSM(SORTING_BUFFER_DATA_SIZE)
  //intervalRR(SORTING_BUFFER_DATA_SIZE)

  if (!fileCreated) {
    for (sbslot ← 0 until SORTING_BUFFER_TOTAL_SLOTS) {
      val pos = sb_slots.get(sbslot)
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
    for (sbslot ← 0 until SORTING_BUFFER_TOTAL_SLOTS) sb_slots.put(sbslot, -1)
    sb_free_slot ++= (0 until SORTING_BUFFER_TOTAL_SLOTS)
    sb_free_space.clear()
    sbMap.clear()
  }

  private def getsb(sbslot: Int): ByteBuffer = {
    val pos = sb_slots.get(sbslot)
    val len = sb_slots_len.get(sbslot)
    val array = new Array[Byte](len)
    sb_data_mmap.position(pos)
    sb_data_mmap.get(array)
    ByteBuffer.wrap(array)
  }

  private def defragment(): Unit = {
    val data = sbMap.map { case (sbslot, slot) ⇒ (getsb(sbslot), slot) }
    resetSlots()
    data.foreach { case (buffer, slot) ⇒ sb_put(buffer, slot) }
  }

  // we can't group operations as they may failed in the middle
  /*
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

  private val flushingBuffer = ByteBuffer.allocateDirect(SORTING_BUFFER_DATA_SIZE)

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
        val pos = sb_slots.get(sbslot)
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
        val pos = sb_slots.get(sbslot)
        val len = sb_slots_len.get(sbslot)
        sbMap -= slot
        sb_free_space.release(pos, len)
        sb_slots.put(sbslot, -1)
    }
  }

  private def flush(): Unit = {
    val iter = sbMap.iterator
    val cs = currSlot.get
    var sidx = 0
    var offset = 0
    val offsets = mutable.ArrayBuffer.empty[Int]
    while (iter.hasNext) {
      val (slot, sbslot) = iter.next()
      while (cs + sidx < slot) {
        offsets += offset
        sidx += 1
      }
      val pos = sb_slots.get(sbslot)
      val len = sb_slots_len.get(sbslot)
      var idx = 0
      while (idx < len) {
        flushingBuffer.put(sb_data_mmap.get(pos + idx))
        idx += 1
      }
      offset += len
      offsets += offset
      sidx += 1
    }
    flushingBuffer.flip()
    putRangeAtForce(flushingBuffer, offsets.toArray, cs)
    flushingBuffer.clear()
    resetSlots()
  }*/

  private def putAtForce(buffer: ByteBuffer, slot: Int): Unit = {
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
  }

  private def sb_put(buffer: ByteBuffer, slot: Int): Unit = {
    if (sb_free_slot.isEmpty) {
      flush()
      putAtViaSortingBuffer(buffer, slot)
    } else {
      val len = buffer.remaining()
      Try(sb_free_space.allocate(len)).recoverWith {
        case _: FragmentationException ⇒
          defragment()
          Try(sb_free_space.allocate(len))
      } match {
        case Success(pos) ⇒
          val sbslot = sb_free_slot.pop()
          sbMap += slot → sbslot
          sb_slots.put(sbslot, pos)
          sb_slots_len.put(sbslot, len)
          sb_slots_map.put(sbslot, slot)
          sb_data_mmap.position(pos)
          sb_data_mmap.put(buffer)
        case Failure(ex) ⇒ ex match {
          case _: NoSpaceLeftException ⇒
            flush()
            putAtViaSortingBuffer(buffer, slot)
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
      Future.successful()
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
    new FileRangeStoreWithSortingBufferV2(toFile, totalSlots)
  }

}