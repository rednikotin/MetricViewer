package database.indexing

import java.nio.ByteBuffer
import database.{FileStore, Store}

trait IndexedStore {
  def addIndex[T](fun: ByteBuffer ⇒ T): Index[T]
  def addUniqueIndex[T](fun: ByteBuffer ⇒ T): UniqueIndex[T]
  def reload(): Unit
}

class IndexedFileStore(val store: FileStore) extends Store {
  private val indexes = scala.collection.mutable.ArrayBuffer[IndexOps]()

  def addIndex[T](fun: ByteBuffer ⇒ T): Index[T] = {
    val idx = new IndexImpl(fun, store)
    indexes += idx
    idx
  }

  def addUniqueIndex[T](fun: ByteBuffer ⇒ T): UniqueIndex[T] = {
    val idx = new UniqueIndexImpl(fun, store)
    indexes += idx
    idx
  }

  private def init() = store.withWriteLock {
    store.iterator().foreach {
      case (id, buffer) ⇒
        indexes.foreach { indexOps ⇒
          indexOps.put(buffer)(id)
        }
    }
  }

  def reload(): Unit = {
    indexes.foreach(indexOps ⇒ indexOps.clear())
    init()
  }

  def insert(buffer: ByteBuffer): Int = store.withWriteLock {
    val verify = indexes.map { indexOps ⇒ indexOps.put(buffer) }
    val id = store.insert(buffer)
    verify.foreach(put ⇒ put(id))
    id
  }

  def update(id: Int, buffer: ByteBuffer): Unit = store.withWriteLock {
    store.check(id)
    val verify = indexes.map { indexOps ⇒ indexOps.update(buffer, id) }
    val oldBuffer = store.select(id).get
    store.update(id, buffer)
    indexes.foreach(indexOps ⇒ indexOps.remove(oldBuffer, id))
    verify.foreach(update ⇒ update())
  }

  def delete(id: Int): Unit = store.withWriteLock {
    store.check(id)
    val oldBuffer = store.select(id).get
    store.delete(id)
    indexes.foreach(indexOps ⇒ indexOps.remove(oldBuffer, id))
  }

  def select(id: Int): Option[ByteBuffer] = store.select(id)

  def iterator(): Iterator[(Int, ByteBuffer)] = store.iterator()

  def truncate(): Unit = store.withWriteLock {
    indexes.foreach(indexOps ⇒ indexOps.clear())
    store.truncate()
  }

  def commit(): Unit = store.commit()

  def check(id: Int): Unit = store.check(id)

  init()
}