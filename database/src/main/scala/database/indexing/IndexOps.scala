package database.indexing

import java.nio.ByteBuffer

trait IndexOps {
  def put(buffer: ByteBuffer): Int ⇒ Unit
  def update(buffer: ByteBuffer, id: Int): () ⇒ Unit
  def remove(buffer: ByteBuffer, id: Int): Unit
  def clear(): Unit
}