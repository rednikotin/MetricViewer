package database

import scala.util.Try

trait IntervalsV7 extends Intervals {
  // built with hash sets + custom double-linked queue for LR hasMap values
  var isDebug = false

  class IntervalSet(left: Int, right: Int) {
    private var length = right - left
    private case class Node(var prev: Node, var next: Node, var left: Int, var right: Int)
    private val intervalsLR_ = new java.util.HashMap[Int, Node]()
    private val intervalsRL = new java.util.HashMap[Int, Int]()
    private var lastNode: Node = null
    private def putNewLR(left: Int, right: Int) = {
      if (lastNode == null) {
        lastNode = Node(null, null, left, right)
      } else {
        val node = Node(lastNode, null, left, right)
        lastNode.next = node
        lastNode = node
      }
      intervalsLR_.put(left, lastNode)
    }
    private def updateLR(left: Int, right: Int) = {
      val node = intervalsLR_.get(left)
      node.right = right
    }
    private def moveLR(oldKey: Int, oldNode: Node, left: Int) = {
      intervalsLR_.remove(oldKey)
      oldNode.left = left
      intervalsLR_.put(left, oldNode)
    }
    def pn(n: Node) = {
      if (n == null) "null" else s"(${n.left}, ${n.right})"
    }

    private def removeLR(key: Int, node: Node) = {
      val node1 = node.prev
      val node2 = node.next
      if (node1 != null) node1.next = node2
      if (node2 != null) node2.prev = node1 else lastNode = node1
      intervalsLR_.remove(key)
    }
    putNewLR(left, right)
    intervalsRL.put(right, left)
    private def release0(left: Int, right: Int): Unit = this.synchronized {
      //if (isDebug) println(s"-> [$left, $right)")
      val leftInterval = intervalsRL.getOrDefault(left, -1)
      val rightIntervalN = intervalsLR_.getOrDefault(right, null)
      length += (right - left)
      if (leftInterval != -1 && rightIntervalN != null) {
        removeLR(right, rightIntervalN)
        intervalsRL.remove(left)
        updateLR(leftInterval, rightIntervalN.right)
        intervalsRL.put(rightIntervalN.right, leftInterval)
      } else if (leftInterval == -1 && rightIntervalN != null) {
        moveLR(right, rightIntervalN, left)
        intervalsRL.put(rightIntervalN.right, left)
      } else if (leftInterval != -1 && rightIntervalN == null) {
        intervalsRL.remove(left)
        updateLR(leftInterval, right)
        intervalsRL.put(right, leftInterval)
      } else {
        putNewLR(left, right)
        intervalsRL.put(right, left)
      }
      //if (isDebug) println(s"[$left, $right), release, getFreeSpace=${getIntervals.mkString("[", ",", "]")}")
    }
    def release(interval: (Int, Int)): Unit = release0(interval._1, interval._2)
    def release(pos: Int, len: Int): Unit = release0(pos, pos + len)

    // can be SLOW as rare executed todo:
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
        //if (isDebug) println(s"length=$length < size=$size, getFreeSpace=${getIntervals.mkString("[", ",", "]")}")
        throw new AllocationFailedException(s"Unable to allocate $size, only $length remained")
      }
      var node = lastNode
      var found = node.right - node.left >= size
      while (node.prev != null && !found) {
        node = node.prev
        found = node.right - node.left >= size
      }
      if (found) {
        val left = node.left
        val right = node.right
        val res = (left, left + size)
        length -= size
        if (res._2 == right) {
          removeLR(left, node)
          intervalsRL.remove(right)
        } else {
          moveLR(left, node, res._2)
          intervalsRL.put(right, res._2)
        }
        //if (isDebug) println(s"length=$length, size=$size, allocated=$res, getFreeSpace=${getIntervals.mkString("[", ",", "]")}")
        res
      } else {
        //if (isDebug) println(s"length=$length, size=$size, fragmentation, getFreeSpace=${getIntervals.mkString("[", ",", "]")}")
        throw new FragmentationException(s"Unable to allocate $size because of fragmentation")
      }
    }

    def tryAllocate(size: Int): Option[(Int, Int)] = Try(allocate(size)).toOption

    def clear(): Unit = {
      length = right - left
      intervalsLR_.clear()
      lastNode = null
      intervalsRL.clear()
      putNewLR(left, right)
      intervalsRL.put(right, left)
    }

    def getIntervals: Iterable[(Int, Int)] = new Iterable[(Int, Int)] {
      override def iterator: Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
        private var cur = lastNode
        def hasNext: Boolean = cur != null
        def next(): (Int, Int) = {
          val res = (cur.left, cur.right)
          cur = cur.prev
          res
        }
      }
    }

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