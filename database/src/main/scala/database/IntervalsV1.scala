package database

import scala.collection.immutable.TreeSet
import scala.util.Try

trait IntervalsV1 extends Intervals {
  // initial implelentation with custom ordering over the immutable treeset

  case class Interval(left: Int, right: Int) { val length: Int = right - left }

  class IntervalSet(left: Int, rigt: Int) {
    private var length = rigt - left
    private var intervals = TreeSet[Interval](Interval(left, rigt))(Ordering.by(_.left))
    def release(interval: Interval): Unit = this.synchronized {
      val leftTree = intervals.to(interval)
      val rightTree = intervals.from(interval)
      if (leftTree.nonEmpty && rightTree.nonEmpty) {
        val leftInterval = leftTree.lastKey
        val rightInterval = rightTree.firstKey
        // 4 cases: non-mergable, both-mergable, left-mergable, right-mergable
        if (leftInterval.right < interval.left && interval.right < rightInterval.left) {
          length += interval.length
          intervals = intervals + interval
        } else if (leftInterval.right == interval.left && interval.right == rightInterval.left) {
          length += interval.length
          intervals = intervals - leftInterval - rightInterval + Interval(leftInterval.left, rightInterval.right)
        } else if (leftInterval.right == interval.left && interval.right < rightInterval.left) {
          length += interval.length
          intervals = intervals - leftInterval + Interval(leftInterval.left, interval.right)
        } else if (leftInterval.right < interval.left && interval.right == rightInterval.left) {
          length += interval.length
          intervals = intervals - rightInterval + Interval(interval.left, rightInterval.right)
        }
      } else if (leftTree.isEmpty && rightTree.nonEmpty) {
        val rightInterval = rightTree.firstKey
        // 2 cases: non-mergable, right-mergable
        if (interval.right < rightInterval.left) {
          length += interval.length
          intervals = intervals + interval
        } else if (interval.right == rightInterval.left) {
          length += interval.length
          intervals = intervals - rightInterval + Interval(interval.left, rightInterval.right)
        }
      } else if (leftTree.nonEmpty && rightTree.isEmpty) {
        val leftInterval = leftTree.lastKey
        // 2 cases: non-mergable, left-mergable
        if (leftInterval.right < interval.left) {
          length += interval.length
          intervals = intervals + interval
        } else if (leftInterval.right == interval.left) {
          length += interval.length
          intervals = intervals - leftInterval + Interval(leftInterval.left, interval.right)
        }
      } else {
        length += interval.length
        intervals = intervals + interval
      }
    }

    def allocated(interval: Interval): Unit = {
      val tree = intervals.to(interval)
      if (tree.nonEmpty) {
        val toSplit = tree.lastKey
        if (toSplit.left == interval.left && interval.right == toSplit.right) {
          intervals = intervals - toSplit
        } else if (toSplit.left == interval.left && interval.right < toSplit.right) {
          intervals = intervals - toSplit + Interval(interval.right, toSplit.right)
        } else if (toSplit.left < interval.left && interval.right == toSplit.right) {
          intervals = intervals - toSplit + Interval(toSplit.left, interval.left)
        } else if (toSplit.left < interval.left && interval.right < toSplit.right) {
          intervals = intervals - toSplit + Interval(toSplit.left, interval.left) + Interval(interval.right, toSplit.right)
        }
      }
    }

    def allocated(pos: Int, len: Int): Unit = allocated(Interval(pos, pos + len))
    def release(pos: Int, len: Int): Unit = release(Interval(pos, pos + len))

    def allocate(size: Int): Interval = this.synchronized {
      if (length < size) {
        throw new AllocationFailedException(s"Unable to allocate $size, only $length remained")
      }
      intervals.find(i ⇒ i.length >= size) match {
        case Some(interval) ⇒
          val res = Interval(interval.left, interval.left + size)
          length -= size
          if (interval.left == res.left && res.right == interval.right) {
            intervals = intervals - interval
          } else {
            intervals = intervals - interval + Interval(res.right, interval.right)
          }
          res
        case None ⇒
          throw new FragmentationException(s"Unable to allocate $size because of fragmentation")
      }
    }

    def tryAllocate(size: Int): Option[Interval] = Try(allocate(size)).toOption

    def clear(): Unit = {
      length = rigt - left
      intervals = TreeSet[Interval](Interval(left, rigt))(Ordering.by(_.left))
    }

    def getIntervals: TreeSet[Interval] = intervals

    def print(): Unit = {
      println("*" * 50)
      intervals.foreach {
        case Interval(x, y) ⇒
          println(s"[$x, $y)")
      }
      println("*" * 50)
    }

    def getLength: Int = length
  }
}
