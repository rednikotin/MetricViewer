package database

import java.io.File
import java.nio.ByteBuffer

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor._
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.collection.JavaConverters._
import database.util.BufferUtil._
import database.util.MemorySeq

class MemoryRangeStoreTest
    extends TestKit(ActorSystem("MemoryRangeStoreTest"))
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  import system.dispatcher

  val arr1: Array[Byte] = Array(1.toByte, 2.toByte, 3.toByte)
  val arr2: Array[Byte] = Array(4.toByte, 5.toByte, 6.toByte, 7.toByte)
  val arr3: Array[Byte] = Array(8.toByte, 9.toByte, 10.toByte, 11.toByte, 12.toByte)

  "MemoryRangeStore InMemory test" must {

    "rs3elem test" in {
      val rs3elem: MemoryRangeStore = RangeStore.createInMemory(30, 1 << 20)
      rs3elem.put(arr1)
      rs3elem.put(arr2)
      rs3elem.put(arr3)

      assert(rs3elem.get(2).get() === 8)
      assert(rs3elem.get(1).limit() === 4)
      assert(rs3elem.get(1).get() === 4)
      assert(rs3elem.get(0).limit() === 3)
    }

    "rs3elemBB test" in {
      val rs3elemBB: MemoryRangeStore = RangeStore.createInMemory(30, 1 << 20)
      rs3elemBB.put(ByteBuffer.wrap(arr3))
      rs3elemBB.put(ByteBuffer.wrap(arr1))
      rs3elemBB.put(ByteBuffer.wrap(arr2))

      assert(rs3elemBB.get(2).get() === 4)
      assert(rs3elemBB.get(1).limit() === 3)
      assert(rs3elemBB.get(1).get() === 1)
      assert(rs3elemBB.get(0).limit() === 5)
    }

    "rsRangeTest1 test" in {
      val rsRangeTest1: MemoryRangeStore = RangeStore.createInMemory(30, 1 << 20)
      rsRangeTest1.put(arr1)
      val arrMid = Array(
        20.toByte, 21.toByte, 22.toByte,
        23.toByte, 24.toByte, 25.toByte, 26.toByte, 27.toByte,
        28.toByte, 29.toByte
      )
      val bbMid = ByteBuffer.wrap(arrMid)
      val offsets = Array(3, 8)
      rsRangeTest1.putRange(bbMid, offsets)
      rsRangeTest1.put(arr2)

      assert(rsRangeTest1.size === 5)
      assert(rsRangeTest1.get(1).get() === 20)
      assert(rsRangeTest1.get(2).get() === 23)
      assert(rsRangeTest1.get(3).limit === 2)
      assert(rsRangeTest1.getRange(1, 3).limit === 10)
      assert(rsRangeTest1.getRangeS(1, 3).length === 10)
      assert(rsRangeTest1.getRange(1, 3).get === 20)
      assert(rsRangeTest1.getRangeS(1, 3).head === 20)
      assert(rsRangeTest1.getRange(1, 3).toSeq(9) === 29)
      assert(rsRangeTest1.getRangeS(1, 3)(9) === 29)
      assert(rsRangeTest1.getRange(1, 500).toSeq.last === 7)
      assert(rsRangeTest1.getRangeS(1, 500).last === 7)
      assert(rsRangeTest1.getRange(200, 500).toSeq === Nil)
      assert(rsRangeTest1.getRangeS(200, 500) === Nil)
      assert(rsRangeTest1.getRange(-1, -2).toSeq === Nil)
      assert(rsRangeTest1.getRangeS(-1, -2) === Nil)
      assert(rsRangeTest1.getRange(-1, 0).get() === 1)
      assert(rsRangeTest1.getRangeS(-1, 0).head === 1)
      assert(rsRangeTest1.getRange(0, 0).get() === 1)
      assert(rsRangeTest1.getRangeS(0, 0).head === 1)
      assert(rsRangeTest1.getRange(4, 5).get() === 4)
      assert(rsRangeTest1.getRangeS(4, 5).head === 4)
    }

    "rsMemSeq test" in {
      val rsMemSeq: MemoryRangeStore = RangeStore.createInMemory(50, 1 << 20)
      for (i ← 1 to 50) rsMemSeq.put(Array(i.toByte))
      val range = rsMemSeq.getRangeS(0, 49)

      assert(rsMemSeq.size === 50)
      range shouldBe a[MemorySeq]
      assert(range.last === 50)
      assert(range.head === 1)
      assert(range.take(3).last === 3)
      assert(range.takeRight(3).head === 48)
      assert(range.drop(10).head === 11)
      assert(range.dropRight(10).last === 40)
      assert(range.iterator.next() === 1)
      assert(range.reverseIterator.next() === 50)
      assert(range.take(49).last === 49)
      assert(range.take(50).last === 50)
      assert(range.take(51).last === 50)
      assert(range.drop(49).head === 50)
      assert(range.drop(50) === Nil)
      assert(range.drop(51) === Nil)
      assert(range.takeRight(49).head === 2)
      assert(range.takeRight(50).head === 1)
      assert(range.takeRight(51).head === 1)
      assert(range.dropRight(49).last === 1)
      assert(range.dropRight(50) === Nil)
      assert(range.dropRight(51) === Nil)
      assert(range.reverse(5) === 45)
      assert(range.reverse.head === 50)
      assert(range.reverse.last === 1)
      assert(range.reverse.length === 50)
      assert(range.reverse(49) === 1)
      assert(range.slice(10, 15) === Seq(11, 12, 13, 14, 15))
      assert(range.slice(0, 1) === Seq(1))
      assert(range.slice(-10, 1) === Seq(1))
      assert(range.slice(40, 500).last === 50)
    }
  }

  "MemoryRangeStore Mapped test" must {

    "rsBigRangeTest2" in {
      val file = new File("data/test4Gb.data")
      val headerSize = 10000
      val arrSize = 1 << 20
      //val fileSize = 1L << 32
      val fileSize = 1L << 28
      val slots = ((fileSize - headerSize * 4) / arrSize).toInt
      val rsBigRangeTest2: MemoryRangeStore = RangeStore.createMapped(headerSize, fileSize, file)
      val arr = new Array[Byte](arrSize)
      for (i ← arr.indices) arr(i) = i.toByte
      for (i ← 1 to slots) {
        rsBigRangeTest2.put(arr)
      }

      assert(rsBigRangeTest2.size === slots)
      assert(rsBigRangeTest2.get(slots / 3).get() === 0)
      assert(rsBigRangeTest2.getRange(slots / 3, slots / 3 + 10).limit() === 11 * arrSize)
      assert(rsBigRangeTest2.getRangeS(slots / 3, slots / 3 + 10).length === 11 * arrSize)
      assert(rsBigRangeTest2.get(slots - 1).toSeq.take(10) === Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
      assert(rsBigRangeTest2.getRange(slots - 2, slots - 1).toSeq.take(10) === Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
      assert(rsBigRangeTest2.getRangeS(slots - 2, slots - 1).take(10) === Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
      assert(rsBigRangeTest2.getRange(slots - 2, slots - 1).limit === 2 * arrSize)
      assert(rsBigRangeTest2.getRangeS(slots - 2, slots - 1).length === 2 * arrSize)

      def getIter: Iterator[ByteBuffer] = rsBigRangeTest2.getRangeIterator(slots / 3, slots / 3 + 10)

      assert(getIter.size === 11)
      assert(getIter.next().length === arrSize)
    }
  }

  override def afterAll(): Unit = {
    shutdown()
  }

}