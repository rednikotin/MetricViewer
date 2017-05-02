package database

import java.io.File
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel

import scala.concurrent.Future

object FileRangeStoreWithSortingBuffer {
  import FileRangeStore._
  val SORTING_BUFFER_DATA_SIZE: Int = 32768
  val SORTING_BUFFER_SLOTS_SIZE: Int = 4096
  val SORTING_BUFFER_TOTAL_SLOTS: Int = SORTING_BUFFER_SLOTS_SIZE / 4
  val SORTING_BUFFER_FIRST_SLOT: Int = RESERVED_LIMIT - SORTING_BUFFER_DATA_SIZE - SORTING_BUFFER_SLOTS_SIZE
  val SORTING_BUFFER_FIRST_SLOT_MAP: Int = SORTING_BUFFER_FIRST_SLOT - SORTING_BUFFER_SLOTS_SIZE
}

class FileRangeStoreWithSortingBuffer(file: File, totalSlots: Int) extends FileRangeStore(file, totalSlots) {
  import FileRangeStore._
  import FileRangeStoreWithSortingBuffer._

  /*
     sorting buffer
     data allowed to put unordered until it fits buffer,
     data saved into sorting buffer if there is a gap to currSlot:
     1. if there is a gap to currSlot, data inserted to SB
     2. if insert has to be done to SB, but SB overflowed, it performed SB flush and added the data to data
     3. if SB is non-empty but data has no gap to currSlot, data inserted to data
     4. if data is to already used slot -- it failed
     5. if after insert data, there is no more gap to some or data in SB, particular flush should be done
     6. particular flush: iterate over SB, saving to data all merged entries, once first gap happened,
        it reads remaining data into memory and reinsert it into SB (we can ignore performance here,
        inserting via SB is not performance oriented, but rather late data handling)
     7. all SB content is beyond readWatermark
     8. possible data loss if JVM crashed before afterGap written back.

     todo: replace with allocation + compactification ?
     todo: persist afterGap in mmap before proceed + add flag and logic to handle
   */

  private val sorting_buffer_slots_map_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, SORTING_BUFFER_FIRST_SLOT_MAP, SORTING_BUFFER_SLOTS_SIZE)
  private val sorting_buffer_slots_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, SORTING_BUFFER_FIRST_SLOT, SORTING_BUFFER_SLOTS_SIZE)
  private val sorting_buffer_data_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, SORTING_BUFFER_FIRST_SLOT + SORTING_BUFFER_SLOTS_SIZE, SORTING_BUFFER_DATA_SIZE)
  def commitSortingBuffer(): Unit = {
    sorting_buffer_slots_map_mmap.force()
    sorting_buffer_slots_mmap.force()
    sorting_buffer_data_mmap.force()
  }

  private val currSlotSortingBuffer = new MMInt(reserved_mmap, 8, if (fileCreated) Some(0) else None)
  private val currPosSortingBuffer = new MMInt(reserved_mmap, 12, if (fileCreated) Some(0) else None)
  private val sorting_buffer_slots = new MMIntArray(sorting_buffer_slots_mmap)
  private val sorting_buffer_slots_map = new MMIntArray(sorting_buffer_slots_map_mmap)
  private val sbMap = collection.mutable.SortedMap.empty[Int, Int]
  if (!fileCreated) {
    for (i ← 0 until currSlotSortingBuffer.get) {
      sbMap += ((sorting_buffer_slots_map.get(i), i))
    }
  }
  private def flushSortingBuffer(isAll: Boolean) = {
    def putAtForce(buffer: ByteBuffer, slot: Int, sbslot: Int) = {
      var retries = 0
      var inserted = false
      // we ignore possible scenario when putAtSync failed with WriteBeyondMaxPosException for now
      while (!inserted) {
        try {
          putAtSync(buffer, slot)
          inserted = true
        } catch {
          case ex: SlotAlreadyUsedException ⇒
            logger.info(s"got SlotAlreadyUsedException(${ex.s}) while flushing sorting buffer (slot=$slot, sbslot=$sbslot)")
          case ex: WriteQueueOverflowException ⇒
            retries += 1
            if (retries > 10) {
              throw new WriteQueueOverflowException(s"Unable to flush buffer, getting exception > 10 times, ${ex.s}")
            }
            Thread.sleep(200)
        }
      }
    }
    val afterGap = collection.mutable.ArrayBuffer.empty[(ByteBuffer, Int)]
    for ((slot, sbslot) ← sbMap) {
      val pos = if (sbslot == 0) 0 else sorting_buffer_slots.get(sbslot - 1)
      val len = sorting_buffer_slots.get(sbslot) - pos

      val array = new Array[Byte](len)
      sorting_buffer_data_mmap.position(pos)
      sorting_buffer_data_mmap.get(array)
      val buffer = ByteBuffer.wrap(array)

      if (isAll || slot == currSlot.get) {
        putAtForce(buffer, slot, sbslot)
      } else {
        afterGap += ((buffer, slot))
      }
    }
    currSlotSortingBuffer.set(0)
    currPosSortingBuffer.set(0)
    sbMap.clear()
    for ((buffer, slot) ← afterGap) {
      putSortingBuffer(buffer, slot)
    }
    //println(s"flush=$isAll, currSlotSortingBuffer=${currSlotSortingBuffer.get}, sbMap.headOption=${sbMap.headOption}")
  }
  private def putSortingBuffer(buffer: ByteBuffer, slot: Int): Unit = {
    val len = buffer.remaining()
    val sbslot = currSlotSortingBuffer.get
    val pos = currPosSortingBuffer.get
    val nextPos = pos.toLong + len
    if (nextPos > SORTING_BUFFER_DATA_SIZE || sbslot >= SORTING_BUFFER_TOTAL_SLOTS) {
      flushSortingBuffer(true)
      putAtAsync(buffer, slot)
    } else {
      currPosSortingBuffer.set(nextPos.toInt)
      sorting_buffer_slots.set(sbslot, nextPos.toInt)
      sorting_buffer_slots_map.set(sbslot, slot)
      sbMap += ((slot, sbslot))
      currSlotSortingBuffer += 1
      sorting_buffer_data_mmap.position(pos)
      sorting_buffer_data_mmap.put(buffer)
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
        flushSortingBuffer(false)
      }
      res.result
    } else {
      putSortingBuffer(bb, idx)
      Future.successful()
    }
  }

  override def shrink(newSize: Int): Unit = writeLock.synchronized {
    currSlotSortingBuffer.set(0)
    currPosSortingBuffer.set(0)
    sbMap.clear()
    super.shrink(newSize)
  }

  override def print(limit: Int = 100) {
    super.print(limit)
    println(s"currPosSortingBuffer=${currPosSortingBuffer.get}, currPosSortingBuffer=${currPosSortingBuffer.get}")
    println(s"sbMap=${sbMap.toList.map(x ⇒ s"${x._1}->${x._2}").mkString(",")}")
    println("*" * 30)
  }
}
