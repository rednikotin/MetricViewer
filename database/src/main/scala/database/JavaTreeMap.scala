package database

import scala.collection.JavaConverters._

// JavaConverters doesn't convert TreeMap as SortedMap (? still correct?)
// I do not implement scala collection traits,
// just emulate required funciton: I don't want to get poor implementation occasionally
class JavaTreeMap[K, V] {
  outer ⇒
  private val map = new java.util.TreeMap[K, V]()
  def clear(): Unit = map.clear()
  def +=(kv: (K, V)): JavaTreeMap[K, V] = {
    map.put(kv._1, kv._2)
    this
  }
  def -=(k: K): JavaTreeMap[K, V] = {
    map.remove(k)
    this
  }
  def headOption: Option[(K, V)] = {
    if (map.isEmpty) None else {
      val head = map.firstEntry()
      Some((head.getKey, head.getValue))
    }
  }
  def lastKey: K = map.lastKey()
  def isEmpty: Boolean = map.isEmpty
  // returned mutable entries! need to copy before user
  def iterator: Iterator[(K, V)] = map.entrySet().iterator().asScala.map(x ⇒ (x.getKey, x.getValue))
  def keys: Iterable[K] = map.keySet().asScala
  def values: Iterable[V] = map.values().asScala
  def map[T](f: ((K, V)) ⇒ T): Iterable[T] = new Iterable[T] {
    def iterator: Iterator[T] = outer.iterator.map(f)
  }
  def toList: List[(K, V)] = iterator.toList
}
