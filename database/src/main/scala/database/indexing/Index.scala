package database.indexing

import java.nio.ByteBuffer
import database.FileStore

trait Index[K] {
  def selectBy(k: K): Iterable[ByteBuffer]
}

class IndexImpl[K](fun: ByteBuffer ⇒ K, store: FileStore) extends Index[K] with IndexOps {
  private val index = scala.collection.mutable.HashMap[K, scala.collection.mutable.HashSet[Int]]()
  def put(buffer: ByteBuffer): Int ⇒ Unit = {
    buffer.mark()
    val k = fun(buffer)
    buffer.reset()
    (id: Int) ⇒
      index.get(k) match {
        case Some(set) ⇒
          set += id
        case None ⇒
          val set = scala.collection.mutable.HashSet(id)
          index += k → set
      }
  }
  def update(buffer: ByteBuffer, id: Int): () ⇒ Unit = {
    buffer.mark()
    val k = fun(buffer)
    buffer.reset()
    () ⇒
      index.get(k) match {
        case Some(set) ⇒
          set += id
        case None ⇒
          val set = scala.collection.mutable.HashSet(id)
          index += k → set
      }
  }
  def remove(buffer: ByteBuffer, id: Int): Unit = {
    buffer.mark()
    val k = fun(buffer)
    buffer.reset()
    index.get(k) match {
      case Some(set) ⇒
        set -= id
        if (set.isEmpty) index -= k
      case None ⇒
    }
  }
  def clear(): Unit = index.clear()
  def selectBy(k: K): Iterable[ByteBuffer] = {
    index.get(k).toIterable.flatMap(set ⇒ set.map(id ⇒ store.select(id).get))
  }
}