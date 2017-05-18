package database.indexing

import java.nio.ByteBuffer
import database.{FileStore, Store}

trait IndexedStore[V] {
  def addIndex[K](fun: V ⇒ K): Index[K, V]
  def addUniqueIndex[K](fun: V ⇒ K): UniqueIndex[K, V]
  def reload(): Unit
}

class IndexedFileStore[V](val store: FileStore, pack: V ⇒ ByteBuffer, unpack: ByteBuffer ⇒ V) extends Store[V] with IndexedStore[V] {
  private val indexes = scala.collection.mutable.ArrayBuffer[IndexOps[V]]()

  def addIndex[K](fun: V ⇒ K): Index[K, V] = {
    val idx = new IndexImpl(fun, store)
    indexes += idx
    idx
  }

  def addUniqueIndex[K](fun: V ⇒ K): UniqueIndex[K, V] = {
    val idx = new UniqueIndexImpl(fun, store)
    indexes += idx
    idx
  }

  private def init() = store.withWriteLock {
    store.iterator().foreach {
      case (id, buffer) ⇒
        val value = unpack(buffer)
        indexes.foreach { indexOps ⇒
          indexOps.put(value)(id)
        }
    }
  }

  def reload(): Unit = {
    indexes.foreach(indexOps ⇒ indexOps.clear())
    init()
  }

  def insert(value: V): Int = store.withWriteLock {
    val verify = indexes.map { indexOps ⇒ indexOps.put(value) }
    val id = store.insert(pack(value))
    verify.foreach(put ⇒ put(id))
    id
  }

  def update(id: Int, value: V): Unit = store.withWriteLock {
    store.check(id)
    val verify = indexes.map { indexOps ⇒ indexOps.update(value, id) }
    val oldValue = unpack(store.select(id).get)
    store.update(id, pack(value))
    indexes.foreach(indexOps ⇒ indexOps.remove(oldValue, id))
    verify.foreach(update ⇒ update())
  }

  def delete(id: Int): Unit = store.withWriteLock {
    store.check(id)
    val oldValue = unpack(store.select(id).get)
    store.delete(id)
    indexes.foreach(indexOps ⇒ indexOps.remove(oldValue, id))
  }

  def select(id: Int): Option[V] = store.select(id).map(unpack)

  def iterator(): Iterator[(Int, V)] = store.iterator().map(x ⇒ (x._1, unpack(x._2)))

  def truncate(): Unit = store.withWriteLock {
    indexes.foreach(indexOps ⇒ indexOps.clear())
    store.truncate()
  }

  def force(): Unit = store.force()

  def check(id: Int): Unit = store.check(id)

  init()
}