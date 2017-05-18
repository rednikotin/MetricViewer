package database.indexing

import database.RWLocks

trait Index[K, V] {
  def select(k: K): Iterable[V]
  def selectId(K: K): Iterable[Int]
}

class IndexImpl[K, V](fun: V ⇒ K, rwlocks: RWLocks) extends Index[K, V] with IndexOps[V] {
  private val index = scala.collection.mutable.HashMap[K, scala.collection.mutable.HashMap[Int, V]]()
  def put(value: V): Int ⇒ Unit = {
    val k = fun(value)
    (id: Int) ⇒
      index.get(k) match {
        case Some(rows) ⇒
          rows += id → value
        case None ⇒
          val rows = scala.collection.mutable.HashMap(id → value)
          index += k → rows
      }
  }
  def update(value: V, id: Int): () ⇒ Unit = {
    val k = fun(value)
    () ⇒
      index.get(k) match {
        case Some(rows) ⇒
          rows += id → value
        case None ⇒
          val rows = scala.collection.mutable.HashMap(id → value)
          index += k → rows
      }
  }
  def remove(value: V, id: Int): Unit = {
    val k = fun(value)
    index.get(k) match {
      case Some(rows) ⇒
        rows -= id
        if (rows.isEmpty) index -= k
      case None ⇒
    }
  }
  def clear(): Unit = index.clear()
  def select(k: K): Iterable[V] = rwlocks.withReadLock(index.get(k).toIterable.flatMap(_.values).toList)
  def selectId(k: K): Iterable[Int] = rwlocks.withReadLock(index.get(k).toIterable.flatMap(_.keySet).toList)
}