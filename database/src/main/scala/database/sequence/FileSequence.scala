package database.sequence

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel

import database.RWLocksImpl

class FileSequence(file: File, pages: Int) {
  final val PAGE_SIZE: Int = 8192
  final val SEQUENCES_PER_PAGE: Int = PAGE_SIZE / 4
  private val raf: RandomAccessFile = new RandomAccessFile(file, "rw")
  private val channel: FileChannel = raf.getChannel
  private val mmaps = (0 until pages).map(i ⇒ {
    val mmap = channel.map(FileChannel.MapMode.READ_WRITE, i * PAGE_SIZE, PAGE_SIZE)
    val inBuffer = mmap.asIntBuffer()
    (mmap, inBuffer, new RWLocksImpl {})
  })

  def getSequence(page: Int, seq: Int): Sequence[Int] = {
    assert(seq >= 0 && seq < SEQUENCES_PER_PAGE)
    assert(page >= 0 && page < pages)
    val (mmap, inBuffer, rwlock) = mmaps(page)
    new Sequence[Int] {
      def curVal(): Int = rwlock.withReadLock(inBuffer.get(seq) - 1)
      def nextVal(): Int = rwlock.withWriteLock {
        val next = inBuffer.get(seq)
        inBuffer.put(seq, next + 1)
        //mmap.force()
        next
      }
      def nextVals(n: Int): Iterable[Int] = rwlock.withWriteLock {
        val next = inBuffer.get(seq)
        inBuffer.put(seq, next + n)
        //mmap.force()
        next.to(next + n)
      }
      def setVal(value: Int): Unit = rwlock.withWriteLock {
        inBuffer.put(seq, value)
        //mmap.force()
      }
      def force(): Unit = mmap.force()
    }
  }

}
