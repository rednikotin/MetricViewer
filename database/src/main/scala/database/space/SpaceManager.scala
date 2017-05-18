package database.space

object SpaceManager {
  class NoSpaceLeftException(msg: String) extends RuntimeException(msg)
  class FragmentationException(msg: String) extends RuntimeException(msg)
}

trait SpaceManager {
  def allocated(allocIntervals: Iterable[(Int, Int)]): Unit
  def allocate(len: Int): Int
  def release(pos: Int, len: Int): Unit
  def clear(): Unit
  def getFreeSpace: Iterator[(Int, Int)]
  def getTotalFreeSpace: Int = getFreeSpace.map(x ⇒ x._2 - x._1).sum
}