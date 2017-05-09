package database

import scala.collection.JavaConverters._
import scala.util.Try

trait IntervalsV5 extends Intervals {
  // java implementation with freelists by size (slightly slower than V4 when space is enough -
  // overhead to keep freelists doesn't covered by reduced defragmentations

  class IntervalSet(left: Int, right: Int) {
    private var length = right - left
    private val intervals = new java.util.TreeMap[Int, Int]()
    private val freelists = new java.util.TreeMap[Int, java.util.TreeSet[Int]]()
    private def put(left: Int, right: Int): Unit = {
      intervals.put(left, right)
      if (freelists.containsKey(right - left)) {
        freelists.get(right - left).add(left)
      } else {
        val freelist = new java.util.TreeSet[Int]()
        freelist.add(left)
        freelists.put(right - left, freelist)
      }
    }
    private def put(interval: (Int, Int)): Unit = put(interval._1, interval._2)
    private def remove(left: Int, right: Int): Unit = {
      intervals.remove(left)
      val freelist = freelists.get(right - left)
      freelist.remove(left)
      if (freelist.isEmpty) freelists.remove(right - left)
    }
    private def remove(interval: java.util.Map.Entry[Int, Int]): Unit = remove(interval.getKey, interval.getValue)
    put(left, right)
    def release(interval: (Int, Int)): Unit = this.synchronized {
      val leftInterval = intervals.floorEntry(interval._1)
      val rightInterval = intervals.ceilingEntry(interval._1)
      if (leftInterval != null && rightInterval != null) {
        // 4 cases: non-mergable, both-mergable, left-mergable, right-mergable
        if (leftInterval.getValue < interval._1 && interval._2 < rightInterval.getKey) {
          length += (interval._2 - interval._1)
          //intervals.put(interval._1, interval._2)
          put(interval)
        } else if (leftInterval.getValue == interval._1 && interval._2 == rightInterval.getKey) {
          length += (interval._2 - interval._1)
          remove(leftInterval)
          remove(rightInterval)
          put(leftInterval.getKey, rightInterval.getValue)
        } else if (leftInterval.getValue == interval._1 && interval._2 < rightInterval.getKey) {
          length += (interval._2 - interval._1)
          remove(leftInterval)
          put(leftInterval.getKey, interval._2)
        } else if (leftInterval.getValue < interval._1 && interval._2 == rightInterval.getKey) {
          length += (interval._2 - interval._1)
          remove(rightInterval)
          put(interval._1, rightInterval.getValue)
        }
      } else if (leftInterval == null && rightInterval != null) {
        // 2 cases: non-mergable, right-mergable
        if (interval._2 < rightInterval.getKey) {
          length += (interval._2 - interval._1)
          put(interval)
        } else if (interval._2 == rightInterval.getKey) {
          length += (interval._2 - interval._1)
          remove(rightInterval)
          put(interval._1, rightInterval.getValue)
        }
      } else if (leftInterval != null && rightInterval == null) {
        // 2 cases: non-mergable, left-mergable
        if (leftInterval.getValue < interval._1) {
          length += (interval._2 - interval._1)
          put(interval)
        } else if (leftInterval.getValue == interval._1) {
          length += (interval._2 - interval._1)
          remove(leftInterval)
          put(leftInterval.getKey, interval._2)
        }
      } else {
        length += (interval._2 - interval._1)
        put(interval)
      }
    }

    def allocated(interval: (Int, Int)): Unit = {
      val toSplit = intervals.floorEntry(interval._1)
      if (toSplit != null) {
        if (toSplit.getKey == interval._1 && interval._2 == toSplit.getValue) {
          remove(toSplit)
        } else if (toSplit.getKey == interval._1 && interval._2 < toSplit.getValue) {
          remove(toSplit)
          put(interval._2, toSplit.getValue)
        } else if (toSplit.getKey < interval._1 && interval._2 == toSplit.getValue) {
          remove(toSplit)
          put(toSplit.getKey, interval._1)
        } else if (toSplit.getKey < interval._1 && interval._2 < toSplit.getValue) {
          remove(toSplit)
          put(toSplit.getKey, interval._1)
          put(interval._2, toSplit.getValue)
        }
      }
    }

    def allocated(pos: Int, len: Int): Unit = allocated(pos → (pos + len))
    def release(pos: Int, len: Int): Unit = release(pos → (pos + len))

    def allocate(size: Int): (Int, Int) = this.synchronized {

      if (length < size) {
        throw new AllocationFailedException(s"Unable to allocate $size, only $length remained")
      }
      val freelist = freelists.ceilingEntry(size)
      if (freelist != null && !freelist.getValue.isEmpty) {
        val left = freelist.getValue.first()
        val right = intervals.get(left)
        val res = (left, left + size)
        length -= size
        if (res._2 == right) {
          remove(left, right)
        } else {
          remove(left, right)
          put(res._2, right)
        }
        res
      } else {
        throw new FragmentationException(s"Unable to allocate $size because of fragmentation")
      }
    }

    def tryAllocate(size: Int): Option[(Int, Int)] = Try(allocate(size)).toOption

    def clear(): Unit = {
      length = right - left
      intervals.clear()
      freelists.clear()
      put(left, right)
    }

    def getIntervals: Iterable[(Int, Int)] = intervals.asScala

    def print(): Unit = {
      println("*" * 50)
      intervals.asScala.foreach {
        case (x, y) ⇒
          println(s"[$x, $y)")
      }
      println("*" * 50)
    }

    def getLength: Int = length
  }
}