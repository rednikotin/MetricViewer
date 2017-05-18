package database

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import scala.collection.mutable
import FileStore._
import database.space.{Intervals, SpaceManager}
import scala.util.{Failure, Success, Try}

trait Store[V] {
  def insert(value: V): Int
  def update(id: Int, value: V): Unit
  def delete(id: Int): Unit
  def delete(ids: Iterable[Int]): Unit = ids.foreach(delete)
  def select(id: Int): Option[V]
  def iterator(): Iterator[(Int, V)]
  def truncate(): Unit
  def force(): Unit
  def check(id: Int): Unit
}

trait RWLocks {
  def withReadLock[T](thunk: ⇒ T): T
  def withWriteLock[T](thunk: ⇒ T): T
}

trait RWLocksImpl extends RWLocks {
  private val rwLock = new java.util.concurrent.locks.ReentrantReadWriteLock()

  def withReadLock[T](thunk: ⇒ T): T = {
    rwLock.readLock().lock()
    val res = Try(thunk)
    rwLock.readLock().unlock()
    res.get
  }

  def withWriteLock[T](thunk: ⇒ T): T = {
    rwLock.writeLock().lock()
    val res = Try(thunk)
    rwLock.writeLock().unlock()
    res.get
  }
}

// table row store
object FileStore {
  class ChecksumMismatchException(msg: String) extends RuntimeException(msg)
  class RowNotExistsException(msg: String) extends RuntimeException(msg)
  class NoRowsLeftException(msg: String) extends RuntimeException(msg)
  class NoSpaceLeftException(msg: String) extends RuntimeException(msg)
  class WriteFailedException(msg: String) extends RuntimeException(msg)
  class ReadFailedException(msg: String) extends RuntimeException(msg)
}

// 1-writer many-readers
class FileStore(val file: File, val maxRows: Int, val maxSize: Int, val reinit: Boolean = false) extends Store[ByteBuffer] with RWLocksImpl {

  private def ceil4k(i: Int) = if (i % 4096 == 0) i else (i / 4096 + 1) * 4096
  private val size4 = ceil4k(maxRows * 4)
  private val size8 = ceil4k(maxRows * 8)
  private val POS_ADDR = 0
  private val LEN_ADDR = POS_ADDR + size4
  private val CHK_ADDR = LEN_ADDR + size4
  private val DAT_ADDR = CHK_ADDR + size8

  protected val initializationRequired: Boolean = !file.exists() || reinit

  private val raf: RandomAccessFile = new RandomAccessFile(file, "rw")
  private val channel: FileChannel = raf.getChannel

  private val pos_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, POS_ADDR, size4)
  private val len_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, LEN_ADDR, size4)
  private val chk_mmap: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, CHK_ADDR, size8)

  private val pos_buffer = pos_mmap.asIntBuffer()
  private val len_buffer = len_mmap.asIntBuffer()
  private val chk_buffer = chk_mmap.asLongBuffer()

  private val free_rows: mutable.Queue[Int] = mutable.Queue[Int]()
  private val free_space: SpaceManager = Intervals.getSpaceManager(maxSize)

  private val bufferPool = BufferPool()
  def releaseBuffer(buffer: ByteBuffer): Unit = bufferPool.release(buffer)

  if (!initializationRequired) withReadLock {
    val allocIntervals = mutable.ArrayBuffer[(Int, Int)]()
    var idx = 0
    while (idx < maxRows) {
      val pos = pos_buffer.get(idx) - 1
      if (pos > -1) {
        val len = len_buffer.get(idx)
        allocIntervals += pos → (pos + len)
      } else {
        free_rows.enqueue(idx)
      }
      idx += 1
    }
    free_space.allocated(allocIntervals)
  }
  else {
    truncate()
  }

  private def insertId(id: Int, buffer: ByteBuffer): Unit = {
    val len = buffer.remaining()
    val chk0 = chk(buffer)
    val pos: Int = Try(free_space.allocate(len)) match {
      case Success(pos_) ⇒ pos_
      case Failure(ex) ⇒ ex match {
        case _: SpaceManager.NoSpaceLeftException ⇒
          throw new NoSpaceLeftException(s"Not enough space ${free_space.getTotalFreeSpace} out of $maxSize")
        case _: SpaceManager.FragmentationException ⇒ throw new NoSpaceLeftException("Defragmentation requires")
        case ex: Throwable                          ⇒ throw ex
      }
    }
    val res = channel.write(buffer, pos + DAT_ADDR)
    if (res != len) {
      throw new WriteFailedException(s"failed to save $len bytes, for id=$id, only $res bytes saved")
    }
    len_buffer.put(id, len)
    chk_buffer.put(id, chk0)
    pos_buffer.put(id, pos + 1)
  }

  def insert(buffer: ByteBuffer): Int = withWriteLock {
    if (free_rows.isEmpty) {
      throw new NoRowsLeftException("all rows are used")
    }
    val id = free_rows.dequeue()
    insertId(id, buffer)
    id
  }

  def delete(id: Int): Unit = withWriteLock {
    val pos = checkPos(id)
    val len = len_buffer.get(id)
    len_buffer.put(id, 0)
    chk_buffer.put(id, 0)
    pos_buffer.put(id, 0)
    free_rows.enqueue(id)
    free_space.release(pos, len)
  }

  def update(id: Int, buffer: ByteBuffer): Unit = withWriteLock {
    val oldPos = checkPos(id)
    val oldLen = len_buffer.get(id)
    val len = buffer.remaining()
    if (len == oldLen) {
      val chk0 = chk(buffer)
      val res = channel.write(buffer, oldPos + DAT_ADDR)
      if (res != len) {
        throw new WriteFailedException(s"failed to save $len bytes, for id=$id, only $res bytes saved")
      }
      chk_buffer.put(id, chk0)
    } else {
      insertId(id, buffer)
      free_space.release(oldPos, oldLen)
    }
  }

  def iterator(): Iterator[(Int, ByteBuffer)] = iterator2().map {
    case (id, chk0, buffer) ⇒
      val chk2 = chk(buffer)
      if (chk0 != chk2) {
        throw new ChecksumMismatchException(s"$chk2 != $chk0")
      }
      (id, buffer)
  }

  def iterator2(): Iterator[(Int, Long, ByteBuffer)] = new Iterator[(Int, Long, ByteBuffer)] {
    private var id = 0
    private def iterate(): Unit = while (id < maxRows && pos_buffer.get(id) == 0) id += 1
    def hasNext: Boolean = withReadLock {
      iterate()
      id < maxRows
    }
    def next(): (Int, Long, ByteBuffer) = withReadLock {
      iterate()
      val thisId = id
      if (thisId < 0) {
        throw new RowNotExistsException(s"no more rows in iterator")
      }
      val len = len_buffer.get(thisId)
      val pos = pos_buffer.get(thisId) - 1
      val chk0 = chk_buffer.get(thisId)
      val buffer = bufferPool.allocate(len)
      val res = channel.read(buffer, pos + DAT_ADDR)
      if (res != len) {
        throw new ReadFailedException(s"failed to read $len bytes, for id=$id, only $res bytes read")
      }
      buffer.flip()
      id += 1
      (thisId, chk0, buffer)
    }
  }

  def truncate(): Unit = withWriteLock {
    var idx = 0
    //raf.setLength(DAT_ADDR)
    free_rows.clear()
    free_space.clear()
    while (idx < maxRows) {
      pos_buffer.put(idx, 0)
      len_buffer.put(idx, 0)
      chk_buffer.put(idx, 0)
      free_rows += idx
      idx += 1
    }
    force()
  }

  def forceMeta(): Unit = {
    chk_mmap.force()
    len_mmap.force()
    pos_mmap.force()
  }

  def force(): Unit = {
    channel.force(false)
  }

  private def checkPos(id: Int): Int = {
    if (id < 0 || id >= maxRows) {
      throw new RowNotExistsException(s"row id=$id is out of [0, ${maxRows - 1}]")
    }
    val pos = pos_buffer.get(id) - 1
    if (pos == -1) {
      throw new RowNotExistsException(s"row with id=$id does't exist")
    }
    pos
  }

  def check(id: Int): Unit = checkPos(id)

  private def checkPos2(id: Int): Option[Int] = {
    if (id < 0 || id >= maxRows) {
      None
    } else {
      val pos = pos_buffer.get(id) - 1
      if (pos == -1) {
        None
      } else {
        Some(pos)
      }
    }
  }

  def select(id: Int): Option[ByteBuffer] = withReadLock {
    checkPos2(id).map { pos ⇒
      val len = len_buffer.get(id)
      val chk0 = chk_buffer.get(id)
      val buffer = bufferPool.allocate(len)
      val res = channel.read(buffer, pos + DAT_ADDR)
      if (res != len) {
        throw new ReadFailedException(s"failed to read $len bytes, for id=$id, only $res bytes read")
      }
      buffer.flip()
      val chk2 = chk(buffer)
      if (chk0 != chk2) {
        throw new ChecksumMismatchException(s"$chk2 != $chk0")
      }
      buffer
    }
  }

  def chk(buffer: ByteBuffer): Long = {
    buffer.mark()
    val checksum = new java.util.zip.CRC32()
    checksum.update(buffer)
    val chk0 = checksum.getValue
    buffer.reset()
    chk0
  }
}