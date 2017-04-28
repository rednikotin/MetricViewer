package database

import java.io.{File, RandomAccessFile}

object RangeStore {
  def createInMemory(totalSlots: Int, allocateSize: Long): MemoryRangeStore = {
    val mb = new MemoryBuffer(allocateSize)
    new MemoryRangeStore(totalSlots, mb)
  }
  def createMapped(totalSlots: Int, totalSize: Long, file: File): MemoryRangeStore = {
    val raf = new RandomAccessFile(file, "rw")
    raf.setLength(totalSize)
    val mb = new MemoryBuffer(raf, totalSize)
    val mrs = new MemoryRangeStore(totalSlots, mb)
    mrs.raf = Some(raf)
    mrs
  }
}

