package database

trait IntervalsV7 extends Intervals {
  // built with hash sets + custom double-linked queue for LR hasMap values
  //var isDebug = false

  class IntervalSet(left: Int, right: Int) {
    outer ⇒
    private var length = right - left
    private case class Node(var prev: Node, var next: Node, var left: Int, var right: Int) { def length: Int = right - left }
    private val intervalsLR = new java.util.HashMap[Int, Node]()
    private val intervalsRL = new java.util.HashMap[Int, Int]()
    private var firstNode: Node = _
    private var lastNode: Node = _
    private var lucky: Node = _
    private def putNewLR(left: Int, right: Int) = {
      if (lastNode == null) {
        lastNode = Node(null, null, left, right)
        firstNode = lastNode
        lucky = lastNode
      } else {
        val node = Node(lastNode, null, left, right)
        if (lucky.length < node.length) lucky = node
        lastNode.next = node
        lastNode = node
      }
      intervalsLR.put(left, lastNode)
    }
    private def updateLR(left: Int, right: Int) = {
      val node = intervalsLR.get(left)
      node.right = right
      if (lucky.length < node.length) lucky = node
    }
    private def moveLR(oldKey: Int, oldNode: Node, left: Int) = {
      intervalsLR.remove(oldKey)
      oldNode.left = left
      if (lucky.length < oldNode.length) lucky = oldNode
      intervalsLR.put(left, oldNode)
    }
    private def removeLR(key: Int, node: Node) = {
      val node1 = node.prev
      val node2 = node.next
      if (node1 != null) node1.next = node2 else firstNode = node2
      if (node2 != null) node2.prev = node1 else lastNode = node1

      if (lucky == node) if (node1 != null) lucky = node1 else lucky = node2
      if (node1 != null && lucky.length < node1.length) lucky = node1
      if (node2 != null && lucky.length < node2.length) lucky = node2

      intervalsLR.remove(key)
    }
    putNewLR(left, right)
    intervalsRL.put(right, left)
    private def release0(left: Int, right: Int): Unit = if (left != right) this.synchronized {
      //if (isDebug) println(s"-> [$left, $right)")
      val leftInterval = intervalsRL.getOrDefault(left, -1)
      val rightIntervalN = intervalsLR.getOrDefault(right, null)
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

    // can be SLOW as rare executed
    def allocated(allocIntervals: Iterable[(Int, Int)]): Unit = {
      val thisIntervals = new java.util.TreeMap[Int, Int]()
      for ((left, right) ← getIntervals) thisIntervals.put(left, right)
      for (interval ← allocIntervals) {
        val toSplit = thisIntervals.floorEntry(interval._1)
        if (toSplit != null) {
          val left = toSplit.getKey
          val right = toSplit.getValue
          length -= (interval._2 - interval._1)
          if (left == interval._1 && interval._2 == right) {
            thisIntervals.remove(left)
            removeLR(left, intervalsLR.get(left))
            intervalsRL.remove(right)
          } else if (left == interval._1 && interval._2 < right) {
            thisIntervals.remove(left)
            thisIntervals.put(interval._2, right)
            moveLR(left, intervalsLR.get(left), interval._2)
            intervalsRL.put(right, interval._2)
          } else if (left < interval._1 && interval._2 == right) {
            thisIntervals.put(left, interval._1)
            updateLR(left, interval._1)
            intervalsRL.remove(right)
            intervalsRL.put(interval._1, left)
          } else if (left < interval._1 && interval._2 < right) {
            thisIntervals.put(left, interval._1)
            thisIntervals.put(interval._2, right)
            updateLR(left, interval._1)
            putNewLR(interval._2, right)
            intervalsRL.put(right, interval._2)
            intervalsRL.put(interval._1, left)
          }
        }
      }
    }

    def allocate(size: Int): (Int, Int) = this.synchronized {
      if (size == 0) return (0, 0)
      if (length < size) {
        //if (isDebug) println(s"length=$length < size=$size, getFreeSpace=${getIntervals.mkString("[", ",", "]")}")
        throw new AllocationFailedException(s"Unable to allocate $size, only $length remained")
      }

      def allocFrom(node: Node) = {
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
      }

      if (lucky != null && lucky.length >= size) {
        allocFrom(lucky)
      } else {
        backIterator.find(node ⇒ node.right - node.left >= size) match {
          case Some(node) ⇒
            allocFrom(node)
          case None ⇒
            //if (isDebug) println(s"length=$length, size=$size, fragmentation, getFreeSpace=${getIntervals.mkString("[", ",", "]")}")
            throw new FragmentationException(s"Unable to allocate $size because of fragmentation")
        }
      }
    }

    def clear(): Unit = {
      length = right - left
      intervalsLR.clear()
      lastNode = null
      intervalsRL.clear()
      putNewLR(left, right)
      intervalsRL.put(right, left)
    }

    private def backIterator = new Iterator[Node] {
      private var cur = lastNode
      def hasNext: Boolean = cur != null
      def next(): Node = {
        val res = cur
        cur = cur.prev
        res
      }
    }

    private def iterator = new Iterator[Node] {
      private var cur = firstNode
      def hasNext: Boolean = cur != null
      def next(): Node = {
        val res = cur
        cur = cur.next
        res
      }
    }

    def getIntervals: Iterable[(Int, Int)] = new Iterable[(Int, Int)] {
      override def iterator: Iterator[(Int, Int)] = outer.iterator.map(node ⇒ (node.left, node.right))
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