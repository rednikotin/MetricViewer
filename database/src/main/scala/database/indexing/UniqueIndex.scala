package database.indexing

import java.nio.ByteBuffer
import database.FileStore
import UniqueIndex._

object UniqueIndex {
  class DuplicatedValueOnIndexException(msg: String) extends RuntimeException(msg)
}

trait UniqueIndex[K] {
  def selectBy(k: K): Option[ByteBuffer]
}

class UniqueIndexImpl[K](fun: ByteBuffer ⇒ K, store: FileStore) extends UniqueIndex[K] with IndexOps {
  private val index = scala.collection.mutable.HashMap[K, Int]()
  def checkPut(buffer: ByteBuffer, id: Int): Unit = {

  }
  def put(buffer: ByteBuffer): Int ⇒ Unit = {
    buffer.mark()
    val k = fun(buffer)
    buffer.reset()
    if (index.contains(k)) {
      val id2 = index(k)
      throw new DuplicatedValueOnIndexException(s"key=$k, id2=$id2")
    }
    (id: Int) ⇒ index += k → id
  }
  def update(buffer: ByteBuffer, id: Int): () ⇒ Unit = {
    buffer.mark()
    val k = fun(buffer)
    buffer.reset()
    if (index.contains(k) && index(k) != id) {
      val id2 = index(k)
      throw new DuplicatedValueOnIndexException(s"key=$k, id2=$id2, id=$id")
    }
    () ⇒ index += k → id
  }
  def remove(buffer: ByteBuffer, id: Int): Unit = {
    buffer.mark()
    val k = fun(buffer)
    buffer.reset()
    index -= k
  }
  def clear(): Unit = index.clear()
  def selectBy(k: K): Option[ByteBuffer] = {
    index.get(k).map(id ⇒ store.select(id).get)
  }
}