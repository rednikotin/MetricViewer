package database

import java.nio.ByteBuffer
import UIndexedStore._

object UIndexedStore {
  class DuplicatedValueOnIndexException(msg: String) extends RuntimeException(msg)
}

trait UIndexedStore extends Store {
  def selectBy(index: Int)(k: Any): Option[ByteBuffer]
}

class UIndexedFileStore(store: Store, funs: (ByteBuffer ⇒ Any)*) extends UIndexedStore {
  private val indexes = funs.map(fun ⇒ (fun, scala.collection.mutable.HashMap[Any, Int]()))

  private def init() = store.withWriteLock {
    store.iterator().foreach {
      case (id, buffer) ⇒
        indexes.foreach {
          case (fun, index) ⇒
            index += fun(buffer) → id
        }
    }
  }

  def insert(buffer: ByteBuffer): Int = store.withWriteLock {
    val ts = indexes.map(i ⇒ (i._1(buffer), i._2))
    if (ts.exists(t ⇒ t._2.contains(t._1))) throw new DuplicatedValueOnIndexException(s"Key already exists")
    val id = store.insert(buffer)
    ts.foreach {
      case (t, index) ⇒
        index += t → id
    }
    id
  }
  def update(id: Int, buffer: ByteBuffer): Unit = store.withWriteLock {
    store.check(id)
    val ts = indexes.map(i ⇒ (i._1(buffer), i._2))
    if (ts.exists(t ⇒ t._2.contains(t._1) && t._2(t._1) != id)) throw new DuplicatedValueOnIndexException(s"Key already exists")
    val oldBuf = store.select(id).get
    val oldts = indexes.map(i ⇒ (i._1(oldBuf), i._2))
    store.update(id, buffer)
    oldts.foreach {
      case (t, index) ⇒
        index -= t
    }
    ts.foreach {
      case (t, index) ⇒
        index += t → id
    }
  }
  def delete(id: Int): Unit = store.withWriteLock {
    store.check(id)
    val oldBuf = store.select(id).get
    val oldts = indexes.map(i ⇒ (i._1(oldBuf), i._2))
    store.delete(id)
    oldts.foreach {
      case (t, index) ⇒
        index -= t
    }
  }
  def select(id: Int): Option[ByteBuffer] = store.select(id)
  def iterator(): Iterator[(Int, ByteBuffer)] = store.iterator()
  def truncate(): Unit = store.withWriteLock {
    indexes.foreach(_._2.clear())
    store.truncate()
  }
  def commit(): Unit = store.commit()
  def check(id: Int): Unit = store.check(id)
  def selectBy(index: Int)(k: Any): Option[ByteBuffer] = store.withReadLock {
    indexes(index)._2.get(k).flatMap(store.select)
  }
  init()
}
