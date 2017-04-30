package database

import java.nio.ByteBuffer

import database.FileRangeStore.PutResult

import scala.concurrent.{ExecutionContext, Future}

trait RangeApi {
  def put(bb: ByteBuffer): Unit
  def putAt(bb: ByteBuffer, idx: Int): Unit
  def get(x: Int): ByteBuffer

  // offsets - start positions of all, except first
  def putRange(bb: ByteBuffer, offsets: Array[Int]): Unit
  def getRange(x: Int, y: Int): ByteBuffer
  def getRangeIterator(x: Int, y: Int): Iterator[ByteBuffer]

  def size: Int
  def print(limit: Int = 100): Unit
}

trait RangeAsyncApi {
  def put(bb: ByteBuffer): PutResult[Int]
  def putAt(bb: ByteBuffer, idx: Int): PutResult[Int]
  def get(x: Int): Future[ByteBuffer]

  // offsets - start positions of all, except first
  def putRange(bb: ByteBuffer, offsets: Array[Int]): PutResult[Int]
  def getRange(x: Int, y: Int): Future[ByteBuffer]
  def getRangeIterator(x: Int, y: Int)(implicit executionContext: ExecutionContext): Future[Iterator[ByteBuffer]]

  def size: Int
  def print(limit: Int = 100): Unit
}
