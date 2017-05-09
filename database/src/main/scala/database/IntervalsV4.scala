package database

import scala.collection.JavaConverters._
import scala.util.Try

trait IntervalsV4 extends Intervals {
  // java implementation without freelists by size

  class IntervalSet(left: Int, right: Int) {
    private var length = right - left
    private val intervals = new java.util.TreeMap[Int, Int]()
    intervals.put(left, right)
    def release(interval: (Int, Int)): Unit = this.synchronized {
      val leftInterval = intervals.floorEntry(interval._1)
      val rightInterval = intervals.ceilingEntry(interval._1)
      if (leftInterval != null && rightInterval != null) {
        // 4 cases: non-mergable, both-mergable, left-mergable, right-mergable
        if (leftInterval.getValue < interval._1 && interval._2 < rightInterval.getKey) {
          length += (interval._2 - interval._1)
          intervals.put(interval._1, interval._2)
        } else if (leftInterval.getValue == interval._1 && interval._2 == rightInterval.getKey) {
          length += (interval._2 - interval._1)
          //intervals.remove(leftInterval.getKey)
          intervals.remove(rightInterval.getKey)
          intervals.put(leftInterval.getKey, rightInterval.getValue)
        } else if (leftInterval.getValue == interval._1 && interval._2 < rightInterval.getKey) {
          length += (interval._2 - interval._1)
          //intervals.remove(leftInterval.getKey)
          intervals.put(leftInterval.getKey, interval._2)
        } else if (leftInterval.getValue < interval._1 && interval._2 == rightInterval.getKey) {
          length += (interval._2 - interval._1)
          intervals.remove(rightInterval.getKey)
          intervals.put(interval._1, rightInterval.getValue)
        }
      } else if (leftInterval == null && rightInterval != null) {
        // 2 cases: non-mergable, right-mergable
        if (interval._2 < rightInterval.getKey) {
          length += (interval._2 - interval._1)
          intervals.put(interval._1, interval._2)
        } else if (interval._2 == rightInterval.getKey) {
          length += (interval._2 - interval._1)
          intervals.remove(rightInterval.getKey)
          intervals.put(interval._1, rightInterval.getValue)
        }
      } else if (leftInterval != null && rightInterval == null) {
        // 2 cases: non-mergable, left-mergable
        if (leftInterval.getValue < interval._1) {
          length += (interval._2 - interval._1)
          intervals.put(interval._1, interval._2)
        } else if (leftInterval.getValue == interval._1) {
          length += (interval._2 - interval._1)
          //intervals.remove(leftInterval.getKey)
          intervals.put(leftInterval.getKey, interval._2)
        }
      } else {
        length += (interval._2 - interval._1)
        intervals.put(interval._1, interval._2)
      }
    }

    def allocated(interval: (Int, Int)): Unit = {
      val toSplit = intervals.floorEntry(interval._1)
      if (toSplit != null) {
        if (toSplit.getKey == interval._1 && interval._2 == toSplit.getValue) {
          intervals.remove(toSplit.getKey)
        } else if (toSplit.getKey == interval._1 && interval._2 < toSplit.getValue) {
          intervals.remove(toSplit.getKey)
          intervals.put(interval._2, toSplit.getValue)
        } else if (toSplit.getKey < interval._1 && interval._2 == toSplit.getValue) {
          //intervals.remove(toSplit.getKey)
          intervals.put(toSplit.getKey, interval._1)
        } else if (toSplit.getKey < interval._1 && interval._2 < toSplit.getValue) {
          //intervals.remove(toSplit.getKey)
          intervals.put(toSplit.getKey, interval._1)
          intervals.put(interval._2, toSplit.getValue)
        }
      }
    }

    def allocated(pos: Int, len: Int): Unit = allocated(pos → (pos + len))
    def release(pos: Int, len: Int): Unit = release(pos → (pos + len))

    def allocate(size: Int): (Int, Int) = this.synchronized {
      if (length < size) {
        throw new AllocationFailedException(s"Unable to allocate $size, only $length remained")
      }
      intervals.entrySet().iterator().asScala.find(i ⇒ (i.getValue - i.getKey) >= size) match {
        case Some(interval0) ⇒
          // interval0 - mutable!!
          val interval = (interval0.getKey, interval0.getValue)
          val res = (interval._1, interval._1 + size)
          length -= size
          if (interval._1 == res._1 && res._2 == interval._2) {
            intervals.remove(interval._1)
          } else {
            intervals.remove(interval._1)
            intervals.put(res._2, interval._2)
          }
          res
        case None ⇒
          throw new FragmentationException(s"Unable to allocate $size because of fragmentation")
      }
    }

    def tryAllocate(size: Int): Option[(Int, Int)] = Try(allocate(size)).toOption

    def clear(): Unit = {
      length = right - left
      intervals.clear()
      intervals.put(left, right)
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