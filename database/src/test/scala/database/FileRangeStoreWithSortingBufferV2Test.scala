package database

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer

import org.scalatest.{BeforeAndAfterAll, Matchers, Tag, WordSpecLike}
import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor._
import BufferUtil._
import database.FileRangeStore._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import MyTags._

import scala.util.{Random, Try}

class FileRangeStoreWithSortingBufferV2Test
    extends TestKit(ActorSystem("FileRangeStoreWithSortingBufferV2Test"))
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  import system.dispatcher

  val slots = 1000
  val filterSlot = 500
  val rnd = new Random
  rnd.setSeed(1)

  val steps: Seq[Int] = (0 until slots).map(i ⇒ 1 + rnd.nextInt(10))
  var j = 0
  var arrays = Seq.empty[Array[Byte]]
  for (step ← steps) {
    arrays = arrays :+ (j until (j + step)).map(_.toByte).toArray
    j += step
  }

  val buffers: Seq[ByteBuffer] = arrays.map(ByteBuffer.wrap)

  val shuffleBuffers: Seq[(ByteBuffer, Int)] = rnd.shuffle(buffers zipWithIndex)

  def rewindBBs(): Unit = buffers.foreach(_.rewind())

  "FileRangeStoreWithSortingBufferV2Test tests" must {
    "inserting random shuffled" taggedAs FileRangeStoreWithSortingBufferV2Test in {
      val fileTest = new File("data/storeSBV2001")
      fileTest.delete()
      val fileStore = new FileRangeStoreWithSortingBufferV2(fileTest, 2 * slots)

      rewindBBs()
      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          fileStore.putAtViaSortingBuffer(bb, slot)
        //println(" -> ", slot, bb.remaining(), bb.toSeq.mkString("[", ", ", "]"))
        //fileStore.print()
      }

      //fileStore.print()

      rewindBBs()

      assert(fileStore.size === slots)

      assert(fileStore.get(2).await().toSeq === buffers(2).toSeq)
      assert(fileStore.get(5).await().limit() === buffers(5).limit())
      assert(fileStore.get(6).await().get() === buffers(6).get())
      assert(fileStore.get(0).await().limit() === buffers(0).limit())

      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          val bb0 = fileStore.get(slot).await()
          assert(bb0.toSeq === bb.toSeq)
          fileStore.releaseBuffer(bb0)

      }
    }

    "copy test" taggedAs FileRangeStoreWithSortingBufferV2Test in {
      val fileTest = new File("data/storeSBV2001-source")
      fileTest.delete()
      val fileStore = new FileRangeStoreWithSortingBufferV2(fileTest, 2 * slots)

      rewindBBs()
      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          fileStore.putAtViaSortingBuffer(bb, slot)
        //println(" -> ", slot, bb.remaining(), bb.toSeq.mkString("[", ", ", "]"))
        //fileStore.print()
      }

      //fileStore.print()

      val fileStore2 = fileStore.copyTo(new File("data/storeSBV2001-copy"))

      rewindBBs()

      assert(fileStore2.size === slots)

      assert(fileStore2.get(2).await().toSeq === buffers(2).toSeq)
      assert(fileStore2.get(5).await().limit() === buffers(5).limit())
      assert(fileStore2.get(6).await().get() === buffers(6).get())
      assert(fileStore2.get(0).await().limit() === buffers(0).limit())

      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          val bb0 = fileStore2.get(slot).await()
          assert(bb0.toSeq === bb.toSeq)
          fileStore2.releaseBuffer(bb0)

      }
    }

    "inserting random shuffled without filterSlot" taggedAs FileRangeStoreWithSortingBufferV2Test in {
      val fileTest = new File("data/storeSBV2002")
      fileTest.delete()
      val fileStore = new FileRangeStoreWithSortingBufferV2(fileTest, 2 * slots)

      rewindBBs()
      shuffleBuffers.filter(_._2 != filterSlot).foreach {
        case (bb, slot) ⇒
          fileStore.putAtViaSortingBuffer(bb, slot)
      }

      //fileStore.print()

      rewindBBs()

      assert(fileStore.size === filterSlot)

      assert(fileStore.get(2).await().toSeq === buffers(2).toSeq)
      assert(fileStore.get(5).await().limit() === buffers(5).limit())
      //assert(fileStore.get(6).await().get() === buffers(6).get())
      assert(fileStore.get(0).await().limit() === buffers(0).limit())

      shuffleBuffers.filter(_._2 < filterSlot).foreach {
        case (bb, slot) ⇒
          val bb0 = fileStore.get(slot).await()
          assert((slot, bb0.toSeq) === (slot, bb.toSeq))
          fileStore.releaseBuffer(bb0)
      }

      shuffleBuffers.filter(_._2 >= filterSlot).foreach {
        case (bb, slot) ⇒
          assertThrows[ReadAboveWatermarkException](fileStore.get(slot))
      }

      rewindBBs()
      fileStore.putAtViaSortingBuffer(buffers(filterSlot), filterSlot)

      rewindBBs()
      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          val bb0 = fileStore.get(slot).await()
          assert((slot, bb0.toSeq) === (slot, bb.toSeq))
          fileStore.releaseBuffer(bb0)
      }
    }

    "inserting random shuffled without 0 slot" taggedAs FileRangeStoreWithSortingBufferV2Test in {
      val fileTest = new File("data/storeSBV2003")
      fileTest.delete()
      val fileStore = new FileRangeStoreWithSortingBufferV2(fileTest, 2 * slots)

      rewindBBs()
      shuffleBuffers.filter(_._2 != 0).foreach {
        case (bb, slot) ⇒
          fileStore.putAtViaSortingBuffer(bb, slot)
      }

      //fileStore.print()

      rewindBBs()

      assert(fileStore.size === 0)

      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          assertThrows[ReadAboveWatermarkException](fileStore.get(slot))
      }

      rewindBBs()
      fileStore.putAtViaSortingBuffer(buffers(0), 0)

      rewindBBs()
      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          val bb0 = fileStore.get(slot).await()
          assert((slot, bb0.toSeq) === (slot, bb.toSeq))
          fileStore.releaseBuffer(bb0)
      }
    }

    "inserting random shuffled async" taggedAs FileRangeStoreWithSortingBufferV2Test in {
      val fileTest = new File("data/storeSBV2004")
      fileTest.delete()
      val fileStore = new FileRangeStoreWithSortingBufferV2(fileTest, 2 * slots)

      rewindBBs()
      Future.sequence(shuffleBuffers.map {
        case (bb, slot) ⇒
          Future(fileStore.putAtViaSortingBuffer(bb, slot))
      }).await()

      rewindBBs()
      assert(fileStore.size === slots)
      assert(fileStore.get(2).await().toSeq === buffers(2).toSeq)
      assert(fileStore.get(5).await().limit() === buffers(5).limit())
      assert(fileStore.get(6).await().get() === buffers(6).get())
      assert(fileStore.get(0).await().limit() === buffers(0).limit())

      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          val bb0 = fileStore.get(slot).await()
          assert((slot, bb0.toSeq) === (slot, bb.toSeq))
          fileStore.releaseBuffer(bb0)
      }
    }

    "inserting more than 1024 unordereds to get gaps" taggedAs FileRangeStoreWithSortingBufferV2Test in {
      val fileTest = new File("data/storeSBV2005")
      fileTest.delete()
      val fileStore = new FileRangeStoreWithSortingBufferV2(fileTest, 2 * slots)

      (1 to 1024).map { i ⇒
        val bb = ByteBuffer.wrap(Array(i.toByte))
        fileStore.putAtViaSortingBuffer(bb, i)
      }

      //fileStore.print()
      assert(fileStore.size === 0)

      fileStore.putAtViaSortingBuffer(ByteBuffer.wrap(Array(1025.toByte)), 1025)

      //fileStore.print(3000)

      assert(fileStore.size === 1026)
      assert(fileStore.get(0).await().toSeq === Nil)
      assert(fileStore.get(666).await().get === 666.toByte)
    }

    "inserting overhread measuring" taggedAs FileRangeStoreWithSortingBufferV2Test in {
      val thisS = 1024
      val shuffle = rnd.shuffle((0 until thisS).toList)
      val array = (0 until 32).map(_.toByte).toArray

      def testWB(): Double = {
        System.gc()
        Thread.sleep(100)
        val fileTest = new File("data/storeSBV2006")
        fileTest.delete()
        val fileStore = new FileRangeStoreWithSortingBufferV2(fileTest, 2 * thisS)
        fileStore.shrink(0)
        val t0 = System.nanoTime()
        shuffle.map { i ⇒
          val bb = ByteBuffer.wrap(array)
          Try(fileStore.putAtViaSortingBuffer(bb, i)).recover { case ex: FileRangeStore.SlotAlreadyUsedException ⇒ }
        }
        val t1 = System.nanoTime()
        t1 - t0
      }

      def testDirect(): Double = {
        System.gc()
        Thread.sleep(100)
        val fileTest = new File("data/storeSBV2007")
        fileTest.delete()
        val fileStore = new FileRangeStoreWithSortingBufferV2(fileTest, 2 * thisS)
        fileStore.shrink(0)
        val t2 = System.nanoTime()
        shuffle.sorted.map { i ⇒
          val bb = ByteBuffer.wrap(array)
          Try(fileStore.putAtSync(bb, i)).recover { case ex: FileRangeStore.SlotAlreadyUsedException ⇒ }
        }
        val t3 = System.nanoTime()
        t3 - t2
      }

      def test(i: Int) = {
        val t1 = testWB() / 1e6
        val t2 = testDirect() / 1e6
        println(s"$i. with buffer = $t1, direct=$t2")
      }

      for (i ← 1 to 10) test(i)
    }

    "heavy inserting" taggedAs HeavyTemporaryTest in {
      val thisS = 1024
      val shuffle = rnd.shuffle((0 until thisS).toList)
      val array = (0 until 32).map(_.toByte).toArray

      System.gc()
      val fileTest = new File("data/storeSBV2006")
      fileTest.delete()
      val fileStore = new FileRangeStoreWithSortingBufferV2(fileTest, 2 * thisS)

      def testWB(): Double = {
        fileStore.shrink(0)
        val t0 = System.nanoTime()
        shuffle.map { i ⇒
          val bb = ByteBuffer.wrap(array)
          Try(fileStore.putAtViaSortingBuffer(bb, i)).recover { case ex: FileRangeStore.SlotAlreadyUsedException ⇒ }
        }
        val t1 = System.nanoTime()
        t1 - t0
      }

      def test(i: Int) = {
        val t1 = testWB() / 1e6
        println(s"$i. with buffer = $t1")
      }

      /*      for (i ← 10.to(0, -1)) {
        println("..." + i)
        Thread.sleep(1000)
      }
      println("Go!!!")*/

      for (i ← 1 to 10) test(i)
    }
  }
}