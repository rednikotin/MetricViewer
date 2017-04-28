package database

import java.nio.ByteBuffer

trait RangeApi {
  def put(arr: Array[Byte]): Unit
  def put(bb: ByteBuffer): Unit
  def get(x: Int): ByteBuffer

  // offsets - start positions of all, except first
  def putRange(bb: ByteBuffer, offsets: Array[Int]): Unit
  def getRange(x: Int, y: Int): ByteBuffer
  def getRangeIterator(x: Int, y: Int): Iterator[ByteBuffer]

  def size: Int
  def print(limit: Int = 100): Unit
}
