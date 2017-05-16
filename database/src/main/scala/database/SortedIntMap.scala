package database

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

// JavaConverters doesn't convert TreeMap as SortedMap (? still correct?)
// I do not implement scala collection traits,
// just emulate required function: I don't want to get poor implementation occasionally
// SortedIntMapJ - wrapper of TreeMap
// SortedIntMapA - array based implementation for dense unordered Int's
// SortedIntMapX - adaptive initially tries to be SortedIntMapA, when expanded more that it can fix, turned to SortedIntMapJ
trait SortedIntMap[V] {
  outer ⇒
  def clear(): Unit
  def +=(kv: (Int, V)): SortedIntMap[V]
  def -=(k: Int): SortedIntMap[V]
  def headOption: Option[(Int, V)]
  def get(k: Int): V
  def lastKey: Int
  def isEmpty: Boolean
  def iterator: Iterator[(Int, V)]
  def keys: Iterable[Int]
  def values: Iterable[V]
  def map[T](f: ((Int, V)) ⇒ T): Iterable[T] = new Iterable[T] {
    def iterator: Iterator[T] = outer.iterator.map(f)
  }
  def toList: List[(Int, V)] = iterator.toList
}

class SortedIntMapJ[V] extends SortedIntMap[V] {
  private val map = new java.util.TreeMap[Int, V]()
  def clear(): Unit = map.clear()
  def +=(kv: (Int, V)): SortedIntMapJ[V] = {
    map.put(kv._1, kv._2)
    this
  }
  def -=(k: Int): SortedIntMapJ[V] = {
    map.remove(k)
    this
  }
  def headOption: Option[(Int, V)] = {
    if (map.isEmpty) None else {
      val head = map.firstEntry()
      Some((head.getKey, head.getValue))
    }
  }
  def get(k: Int): V = map.get(k)
  def lastKey: Int = map.lastKey()
  def isEmpty: Boolean = map.isEmpty
  // returned mutable entries! need to copy before use
  def iterator: Iterator[(Int, V)] = map.entrySet().iterator().asScala.map(x ⇒ (x.getKey, x.getValue))
  def keys: Iterable[Int] = map.keySet().asScala
  def values: Iterable[V] = map.values().asScala
}

class SortedIntMapA[V: ClassTag](unused: V) extends SortedIntMap[V] {
  outer ⇒

  private val MAX_ARRAY_SIZE = 1 << 15
  private val array = new Array[V](MAX_ARRAY_SIZE)
  private var needSet = true
  private var head: (Int, V) = (0, unused)
  private var last: (Int, V) = (0, unused)
  private var shift = 0

  private def checkHT(): Unit =
    if (needSet) {
      val firstIdx = head._1 + shift
      val lastIdx = last._1 + shift
      var idx = firstIdx
      while (idx <= lastIdx && array(idx) == unused) idx += 1
      if (idx <= lastIdx) {
        head = (idx - shift, array(idx))
        idx = lastIdx
        while (idx >= firstIdx && array(idx) == unused) idx -= 1
        last = (idx - shift, array(idx))
      } else {
        head = (0, unused)
        last = (0, unused)
      }
      needSet = false
    }

  def init(): Unit = {
    var idx = 0
    while (idx < MAX_ARRAY_SIZE) {
      array(idx) = unused
      idx += 1
    }
  }

  def clear(): Unit = if (!isEmpty) {
    var idx = head._1 + shift
    val lastIdx = last._1 + shift
    while (idx <= lastIdx) {
      array(idx) = unused
      idx += 1
    }
    head = (0, unused)
    last = (0, unused)
    needSet = false
  }

  def canBeAdded(k: Int): Boolean = head._2 == unused || (k + shift >= 0 && k + shift < MAX_ARRAY_SIZE)

  def +=(kv: (Int, V)): SortedIntMapA[V] = {
    checkHT()
    if (head._2 == unused) {
      shift = MAX_ARRAY_SIZE / 2 - kv._1
      head = kv
      last = kv
    } else {
      if (head._1 > kv._1) head = kv
      if (last._1 < kv._1) last = kv
    }
    array(kv._1 + shift) = kv._2
    this
  }
  def -=(k: Int): SortedIntMap[V] = {
    needSet = needSet || k == head._1 || k == last._1
    array(k + shift) = unused
    this
  }

  def headOption: Option[(Int, V)] = if (isEmpty) None else {
    checkHT()
    Some(head)
  }

  def get(k: Int): V = {
    val res = array(k + shift)
    if (res == unused) throw new NoSuchElementException
    res
  }

  def lastKey: Int = if (isEmpty) throw new NoSuchElementException else {
    checkHT()
    last._1
  }

  def isEmpty: Boolean = head._2 == unused

  def iterator: Iterator[(Int, V)] = new Iterator[(Int, V)] {
    checkHT()
    private var idx = head._1 + shift
    private val lastIdx = last._1 + shift
    def hasNext: Boolean = if (outer.isEmpty) {
      false
    } else {
      while (idx <= lastIdx && array(idx) == unused) {
        idx += 1
      }
      idx <= lastIdx
    }
    def next(): (Int, V) = {
      var value = array(idx)
      while (idx <= lastIdx && value == unused) {
        idx += 1
        value = array(idx)
      }
      if (idx > lastIdx) throw new NoSuchElementException
      val res = (idx - shift, value)
      idx += 1
      res

    }
  }

  def keys: Iterable[Int] = new Iterable[Int] {
    def iterator: Iterator[Int] = outer.iterator.map(_._1)
  }

  def values: Iterable[V] = new Iterable[V] {
    def iterator: Iterator[V] = outer.iterator.map(_._2)
  }

  init()
}

class SortedIntMapX[V: ClassTag](unused: V) extends SortedIntMap[V] {
  @volatile private var switched = 0
  def getSwitched: Int = switched
  def resetStats(): Unit = switched = 0
  private val mapa = new SortedIntMapA[V](unused)
  private val mapj = new SortedIntMapJ[V]()
  private var aorj = true
  private def cur = if (aorj) mapa else mapj
  def clear(): Unit = {
    cur.clear()
    aorj = true
  }
  def +=(kv: (Int, V)): SortedIntMap[V] = {
    if (aorj) {
      if (mapa.canBeAdded(kv._1)) {
        mapa += kv
      } else {
        switched += 1
        mapa.iterator.foreach(mapj.+=)
        aorj = false
        mapa.clear()
        mapj += kv
      }
    } else {
      mapj += kv
    }
    this
  }
  def -=(k: Int): SortedIntMap[V] = {
    if (aorj) {
      mapa -= k
    } else {
      mapj -= k
      if (mapj.isEmpty) {
        aorj = true
      }
    }
    this
  }
  def headOption: Option[(Int, V)] = cur.headOption
  def get(k: Int): V = cur.get(k)
  def lastKey: Int = cur.lastKey
  def isEmpty: Boolean = cur.isEmpty
  def iterator: Iterator[(Int, V)] = cur.iterator
  def keys: Iterable[Int] = cur.keys
  def values: Iterable[V] = cur.values
}