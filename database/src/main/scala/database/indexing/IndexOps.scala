package database.indexing

// must be executed withing Write lock of the same lock which Read happened
trait IndexOps[V] {
  def put(value: V): Int ⇒ Unit
  def update(value: V, id: Int): () ⇒ Unit
  def remove(value: V, id: Int): Unit
  def clear(): Unit
}