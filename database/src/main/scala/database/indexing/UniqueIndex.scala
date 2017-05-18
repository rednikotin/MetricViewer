package database.indexing

import database.RWLocks
import UniqueIndex._

object UniqueIndex {
  class DuplicatedValueOnIndexException(msg: String) extends RuntimeException(msg)
}

trait UniqueIndex[K, V] {
  def select(k: K): Option[V]
  def selectId(k: K): Option[Int]
}

class UniqueIndexImpl[K, V](fun: V ⇒ K, rwlocks: RWLocks) extends UniqueIndex[K, V] with IndexOps[V] {
  private val index = scala.collection.mutable.HashMap[K, (Int, V)]()
  def put(value: V): Int ⇒ Unit = {
    val k = fun(value)
    if (index.contains(k)) {
      val (id2, _) = index(k)
      throw new DuplicatedValueOnIndexException(s"key=$k, id2=$id2")
    }
    (id: Int) ⇒ index += k → (id, value)
  }
  def update(value: V, id: Int): () ⇒ Unit = {
    val k = fun(value)
    if (index.contains(k) && index(k)._1 != id) {
      val id2 = index(k)
      throw new DuplicatedValueOnIndexException(s"key=$k, id2=$id2, id=$id")
    }
    () ⇒ index += k → (id, value)
  }
  def remove(value: V, id: Int): Unit = {
    val k = fun(value)
    index -= k
  }
  def clear(): Unit = index.clear()
  def select(k: K): Option[V] = rwlocks.withReadLock(index.get(k).map(_._2))
  def selectId(k: K): Option[Int] = rwlocks.withReadLock(index.get(k).map(_._1))
}