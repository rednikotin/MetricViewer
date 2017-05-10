package database

import scala.collection.JavaConverters._
import scala.util.Try

trait IntervalsV6 extends Intervals {
  // built with hash sets instead, unexpectedly  slow on iterators!

  class IntervalSet(left: Int, right: Int) {
    private var length = right - left
    private val intervalsLR = new java.util.HashMap[Int, Int]()
    private val intervalsRL = new java.util.HashMap[Int, Int]()
    intervalsLR.put(left, right)
    intervalsRL.put(right, left)
    private def release0(left: Int, right: Int): Unit = this.synchronized {
      val leftInterval = intervalsRL.getOrDefault(left, -1)
      val rightInterval = intervalsLR.getOrDefault(right, -1)
      length += (right - left)
      if (leftInterval != -1 && rightInterval != -1) {
        intervalsLR.remove(right)
        intervalsRL.remove(left)
        intervalsLR.put(leftInterval, rightInterval)
        intervalsRL.put(rightInterval, leftInterval)
      } else if (leftInterval == -1 && rightInterval != -1) {
        intervalsLR.remove(right)
        intervalsLR.put(left, rightInterval)
        intervalsRL.put(rightInterval, left)
      } else if (leftInterval != -1 && rightInterval == -1) {
        intervalsRL.remove(left)
        intervalsLR.put(leftInterval, right)
        intervalsRL.put(right, leftInterval)
      } else {
        intervalsLR.put(left, right)
        intervalsRL.put(right, left)
      }
      //println(s"[$left, $right), release, getFreeSpace=${getIntervals.mkString("[", ",", "]")}")
    }
    def release(interval: (Int, Int)): Unit = release0(interval._1, interval._2)
    def release(pos: Int, len: Int): Unit = release0(pos, pos + len)

    // can be SLOW as rare executed
    def allocated(interval: (Int, Int)): Unit = {
      /*      val toSplit = intervals.floorEntry(interval._1)
      if (toSplit != null) {
        if (toSplit.getKey == interval._1 && interval._2 == toSplit.getValue) {
          intervals.remove(toSplit.getKey)
        } else if (toSplit.getKey == interval._1 && interval._2 < toSplit.getValue) {
          intervals.remove(toSplit.getKey)
          intervals.put(interval._2, toSplit.getValue)
        } else if (toSplit.getKey < interval._1 && interval._2 == toSplit.getValue) {
          //
          intervals.remove(toSplit.getKey)
          intervals.put(toSplit.getKey, interval._1)
        } else if (toSplit.getKey < interval._1 && interval._2 < toSplit.getValue) {
          //
          intervals.remove(toSplit.getKey)
          intervals.put(toSplit.getKey, interval._1)
          intervals.put(interval._2, toSplit.getValue)
        }
      }*/
    }

    def allocate(size: Int): (Int, Int) = this.synchronized {
      if (length < size) {
        //println(s"length=$length < size=$size, getFreeSpace=${getIntervals.mkString("[", ",", "]")}")
        throw new AllocationFailedException(s"Unable to allocate $size, only $length remained")
      }
      intervalsLR.entrySet().iterator().asScala.find(i ⇒ (i.getValue - i.getKey) >= size) match {
        case Some(interval) ⇒
          // interval0 - mutable!!
          val left = interval.getKey
          val right = interval.getValue
          val res = (left, left + size)
          length -= size
          if (res._2 == right) {
            intervalsLR.remove(left)
            intervalsRL.remove(right)
          } else {
            intervalsLR.remove(left)
            intervalsLR.put(res._2, right)
            intervalsRL.put(right, res._2)
          }
          //println(s"length=$length, size=$size, allocated=$res, getFreeSpace=${getIntervals.mkString("[", ",", "]")}")
          res
        case None ⇒
          //println(s"length=$length, size=$size, fragmentation, getFreeSpace=${getIntervals.mkString("[", ",", "]")}")
          throw new FragmentationException(s"Unable to allocate $size because of fragmentation")
      }
    }

    def tryAllocate(size: Int): Option[(Int, Int)] = Try(allocate(size)).toOption

    def clear(): Unit = {
      length = right - left
      intervalsLR.clear()
      intervalsRL.clear()
      intervalsLR.put(left, right)
      intervalsRL.put(right, left)
    }

    def getIntervals: Iterable[(Int, Int)] = intervalsLR.asScala.toSeq.sorted

    def print(): Unit = {
      println("*" * 50)
      getIntervals.foreach {
        case (x, y) ⇒
          println(s"[$x, $y)")
      }
      println("*" * 50)
    }

    def getLength: Int = length
  }
}