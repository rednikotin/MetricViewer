package database

import SpaceManager._
trait Intervals {
  class AllocationFailedException(val msg: String) extends RuntimeException(msg)
  class FragmentationException(val msg: String) extends RuntimeException(msg)
}

object Intervals extends IntervalsV7 {

  def getSpaceManager(size: Int): SpaceManager = {
    new SpaceManager {
      private val intervals = new IntervalSet(0, size)

      def allocated(allocIntervals: Iterable[(Int, Int)]): Unit = intervals.allocated(allocIntervals)

      def allocate(len: Int): Int = try {
        intervals.allocate(len)._1
      } catch {
        case ex: Intervals.AllocationFailedException ⇒
          throw new SpaceManager.NoSpaceLeftException(ex.msg)
        case ex: Intervals.FragmentationException ⇒
          throw new SpaceManager.FragmentationException(ex.msg)
      }

      def release(pos: Int, len: Int): Unit = intervals.release(pos, len)

      def clear(): Unit = intervals.clear()

      def getFreeSpace: Iterator[(Int, Int)] = intervals.getIntervals.iterator
    }
  }
}