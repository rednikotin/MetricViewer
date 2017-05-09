package database

trait Intervals {
  class AllocationFailedException(val msg: String) extends RuntimeException(msg)
  class FragmentationException(val msg: String) extends RuntimeException(msg)
}

object Intervals extends IntervalsV4