package database.space

import database.space.Intervals._

import scala.util.Try

trait IntervalsV3 extends Intervals {

  class IntervalSet(left: Int, right: Int) {
    // !!! but in 12.2.1 -- prevent to use this!

    private var length = right - left
    private val intervals = collection.mutable.TreeMap[Int, Int](left → right)
    def release(interval: (Int, Int)): Unit = this.synchronized {
      val leftTree = intervals.to(interval._1)
      val rightTree = intervals.from(interval._1)
      println(interval, leftTree, intervals)

      if (leftTree.nonEmpty && rightTree.nonEmpty) {
        val leftInterval = leftTree.last
        val rightInterval = rightTree.head
        // 4 cases: non-mergable, both-mergable, left-mergable, right-mergable
        if (leftInterval._2 < interval._1 && interval._2 < rightInterval._1) {
          length += (interval._2 - interval._1)
          intervals += interval
        } else if (leftInterval._2 == interval._1 && interval._2 == rightInterval._1) {
          length += (interval._2 - interval._1)
          intervals /*-= leftInterval._1*/ -= rightInterval._1 += leftInterval._1 → rightInterval._2
        } else if (leftInterval._2 == interval._1 && interval._2 < rightInterval._1) {
          length += (interval._2 - interval._1)
          intervals /*-= leftInterval._1*/ += leftInterval._1 → interval._2
        } else if (leftInterval._2 < interval._1 && interval._2 == rightInterval._1) {
          length += (interval._2 - interval._1)
          intervals -= rightInterval._1 += interval._1 → rightInterval._2
        }
      } else if (leftTree.isEmpty && rightTree.nonEmpty) {
        val rightInterval = rightTree.head
        // 2 cases: non-mergable, right-mergable
        if (interval._2 < rightInterval._1) {
          length += (interval._2 - interval._1)
          intervals += interval
        } else if (interval._2 == rightInterval._1) {
          length += (interval._2 - interval._1)
          intervals -= rightInterval._1 += interval._1 → rightInterval._2
        }
      } else if (leftTree.nonEmpty && rightTree.isEmpty) {
        val leftInterval = leftTree.last
        // 2 cases: non-mergable, left-mergable
        if (leftInterval._2 < interval._1) {
          length += (interval._2 - interval._1)
          intervals += interval
        } else if (leftInterval._2 == interval._1) {
          length += (interval._2 - interval._1)
          intervals /*-= leftInterval._1*/ += leftInterval._1 → interval._2
        }
      } else {
        length += (interval._2 - interval._1)
        intervals += interval
      }
    }

    def allocated(interval: (Int, Int)): Unit = {
      val tree = intervals.to(interval._1)
      if (tree.nonEmpty) {
        val toSplit = tree.last
        if (toSplit._1 == interval._1 && interval._2 == toSplit._2) {
          intervals -= toSplit._1
        } else if (toSplit._1 == interval._1 && interval._2 < toSplit._2) {
          intervals -= toSplit._1 += interval._2 → toSplit._2
        } else if (toSplit._1 < interval._1 && interval._2 == toSplit._2) {
          intervals /*-= toSplit._1*/ += toSplit._1 → interval._1
        } else if (toSplit._1 < interval._1 && interval._2 < toSplit._2) {
          intervals /*-= toSplit._1*/ += toSplit._1 → interval._1 += interval._2 → toSplit._2
        }
      }
    }

    def allocated(pos: Int, len: Int): Unit = allocated(pos → (pos + len))
    def release(pos: Int, len: Int): Unit = release(pos → (pos + len))

    def allocate(size: Int): (Int, Int) = this.synchronized {
      if (length < size) {
        throw new AllocationFailedException(s"Unable to allocate $size, only $length remained")
      }
      intervals.find(i ⇒ (i._2 - i._1) >= size) match {
        case Some(interval) ⇒
          val res = (interval._1, interval._1 + size)
          length -= size
          if (interval._1 == res._1 && res._2 == interval._2) {
            intervals -= interval._1
          } else {
            intervals -= interval._1 += res._2 → interval._2
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
      intervals += left → right
    }

    def getIntervals: Iterable[(Int, Int)] = intervals

    def print(): Unit = {
      println("*" * 50)
      intervals.foreach {
        case (x, y) ⇒
          println(s"[$x, $y)")
      }
      println("*" * 50)
    }

    def getLength: Int = length
  }
}