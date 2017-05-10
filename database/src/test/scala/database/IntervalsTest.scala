package database

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, IntBuffer}

import org.scalatest.{BeforeAndAfterAll, Matchers, Tag, WordSpecLike}
import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor._
import BufferUtil._
import database.FileRangeStore._

import scala.concurrent.Future
import MyTags._

import scala.util.{Failure, Random, Success, Try}

class IntervalsTest
    extends TestKit(ActorSystem("IntervalsTest"))
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  import system.dispatcher

  //IntervalsImplTest

  "intervals operations" must {
    "be correct v1" taggedAs IntervalsImplTest in {
      val impl: IntervalsV1 = new IntervalsV1 {}
      val intervals = new impl.IntervalSet(0, 10)

      val i1 = intervals.allocate(3)
      val i2 = intervals.allocate(3)
      val i3 = intervals.allocate(3)
      assert(i1 === impl.Interval(0, 3))
      assert(i2 === impl.Interval(3, 6))
      assert(i3 === impl.Interval(6, 9))
      assert(intervals.getLength === 1)
      intervals.release(i2)
      assert(intervals.getLength === 4)
      assertThrows[impl.FragmentationException](intervals.allocate(4))
      assert(intervals.allocate(2) === impl.Interval(3, 5))
    }

    "be correct default version" taggedAs IntervalsImplTest in {
      val intervals = new Intervals.IntervalSet(0, 10)

      val i1 = intervals.allocate(3)
      val i2 = intervals.allocate(3)
      val i3 = intervals.allocate(3)
      assert(i1 === (0, 3))
      assert(i2 === (3, 6))
      assert(i3 === (6, 9))
      assert(intervals.getLength === 1)
      intervals.release(i2)
      assert(intervals.getLength === 4)
      assertThrows[Intervals.FragmentationException](intervals.allocate(4))
      assert(intervals.allocate(2) === (3, 5))
    }

    "be correct default version spec case (merging released intervals)" taggedAs IntervalsImplTest in {
      val intervals = new Intervals.IntervalSet(0, 100)

      val i1 = intervals.allocate(9) // 0-9
      val i2 = intervals.allocate(8) // 9-17
      val i3 = intervals.allocate(10) // 17-27
      val i4 = intervals.allocate(9) // 27-36
      val i5 = intervals.allocate(8) // 36-44
      val i6 = intervals.allocate(5) // 44-49
      val i7 = intervals.allocate(4) // 49-53
      val i8 = intervals.allocate(7) // 53-60
      val i9 = intervals.allocate(5) // 60-65
      val i10 = intervals.allocate(5) // 65-70
      intervals.release(i1)
      intervals.release(i4)
      intervals.release(i5)
      intervals.release(i8)
      intervals.release(i9)

      //[(0,9),(27,36),(36,44),(53,65),(72,77),(88,93),(152,156),(182,524288)]

      assert(intervals.getIntervals.toSeq.sorted == Seq((0, 9), (27, 44), (53, 65), (70, 100)))

      intervals.clear()
      intervals.allocate(9) // 0-9
      intervals.allocate(8) // 9-17
      intervals.allocate(10) // 17-27
      intervals.allocate(9) // 27-36
      intervals.allocate(8) // 36-44
      intervals.allocate(5) // 44-49
      intervals.allocate(4) // 49-53
      intervals.allocate(7) // 53-60
      intervals.allocate(5) // 60-65
      intervals.allocate(5) // 65-70
      intervals.release(i1)
      intervals.release(i5)
      intervals.release(i8)
      intervals.release(i4)
      intervals.release(i9)
      val i11 = intervals.allocate(8)
      intervals.release(i6)
      intervals.release(i7)
      val i12 = intervals.allocate(3)

      //println(intervals.getIntervals.toSeq)

      //assert(Set(Seq((8, 9), (27, 65), (73, 100)), Seq((8, 9), (30, 65), (70, 100))).contains(intervals.getIntervals.toSeq.sorted))
      assert(Seq((8, 9), (27, 65), (73, 100)).map(x ⇒ x._2 - x._1).sum === intervals.getIntervals.map(x ⇒ x._2 - x._1).sum)
    }

    "test IntervalsImplPrintTest" taggedAs IntervalsImplPrintTest in {
      val impl: IntervalsV4 = new IntervalsV4 {}
      val intervals = new impl.IntervalSet(0, 10)

      val i1 = intervals.allocate(3)
      val i2 = intervals.allocate(3)
      val i3 = intervals.allocate(3)
      assert(i1 === (0, 3))
      assert(i2 === (3, 6))
      assert(i3 === (6, 9))
      assert(intervals.getLength === 1)
      intervals.release(i2)
      assert(intervals.getLength === 4)
      assertThrows[impl.FragmentationException](intervals.allocate(4))
      assert(intervals.allocate(2) === (3, 5))
    }
  }

}
