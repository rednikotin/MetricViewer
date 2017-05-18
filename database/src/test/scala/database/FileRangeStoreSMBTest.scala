package database

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, IntBuffer}
import org.scalatest.{BeforeAndAfterAll, Matchers, Tag, WordSpecLike}
import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor._
import database.util.BufferUtil._
import database.FileRangeStore._
import scala.concurrent.Future
import MyTags._
import database.FileRangeStore.TrashMeta

import scala.util.{Failure, Random, Success, Try}

class FileRangeStoreSMBTest
    extends TestKit(ActorSystem("FileRangeStoreSMBTest"))
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  import system.dispatcher

  val maxSlot = FileRangeStore.SORTING_BUFFER_TOTAL_SLOTS
  val slots = maxSlot - 24
  val filterSlot = 500
  val rnd = new Random
  rnd.setSeed(1)

  val steps: Seq[Int] = (0 until slots).map(i ⇒ 1 + rnd.nextInt(10))
  var j = 0
  lazy val arrays = {
    var arrays = Seq.empty[Array[Byte]]
    for (step ← steps) {
      arrays = arrays :+ (j until (j + step)).map(_.toByte).toArray
      j += step
    }
    arrays
  }

  lazy val buffers: Seq[ByteBuffer] = arrays.map(ByteBuffer.wrap)

  lazy val shuffleBuffers: Seq[(ByteBuffer, Int)] = rnd.shuffle(buffers zipWithIndex)

  def rewindBBs(): Unit = buffers.foreach(_.rewind())

  "FileRangeStoreMSBTest tests" must {
    "inserting random shuffled" taggedAs (FileRangeStoreMSBTest, WeirdSMB3) in {
      val fileTest = new File("data/storeSBV2001-SMB")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * slots, 500000)

      rewindBBs()
      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          fileStore.putAtViaSortingBuffer(bb, slot)
      }

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

    "copy test" taggedAs FileRangeStoreMSBTest in {
      val fileTest = new File("data/storeSBV2001-source-SMB")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * slots, 5000)

      rewindBBs()
      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          fileStore.putAtViaSortingBuffer(bb, slot)
        //println(" -> ", slot, bb.remaining(), bb.toSeq.mkString("[", ", ", "]"))
        //fileStore.print()
      }

      //fileStore.print()

      val fileStore2 = fileStore.copyTo(new File("data/storeSBV2001-copy-SMB"))

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

    "inserting random shuffled without filterSlot" taggedAs FileRangeStoreMSBTest in {
      val fileTest = new File("data/storeSBV2002-SMB")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * slots, 5000)

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

    "inserting random shuffled without 0 slot" taggedAs FileRangeStoreMSBTest in {
      val fileTest = new File("data/storeSBV2003-SMB")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * slots, 5000)

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

    "inserting random shuffled async" taggedAs FileRangeStoreMSBTest in {
      val fileTest = new File("data/storeSBV2004-SMB")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * slots, 5000)

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

    "flushing and last put cases" taggedAs (FileRangeStoreMSBTest, SBFlush) in {
      val sb_slots = SORTING_BUFFER_TOTAL_SLOTS
      val sb_size = SORTING_BUFFER_DATA_SIZE
      val sb_aux = SORTING_BUFFER_FLUSH_AUX

      //println(s"sb_slots=$sb_slots, sb_size=$sb_size, sd_aux=$sb_aux")

      val fileTest = new File("data/storeSBV2014-SMB")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * slots, 50)

      {
        // insert 10(small), 20(big), 15(small)
        val bb1 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb1, 10).await()
        val bb2 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb2, 20).await()
        val bb3 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb3, 15).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 21)
        assert(fileStore.get(15).await().length === 10)
        fileStore.shrink(0)
        fileStore.resetCounters()

        // insert 10(small), 20(big), 15(exact-1)
        val bb11 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb11, 10).await()
        val bb21 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb21, 20).await()
        val bb31 = ByteBuffer.wrap(new Array[Byte](sb_aux - 1))
        fileStore.putAtViaSortingBuffer(bb31, 15).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 21)
        assert(fileStore.get(15).await().length === (sb_aux - 1))
        fileStore.shrink(0)
        fileStore.resetCounters()

        // insert 10(small), 20(big), 15(exact)
        val bb4 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb4, 10).await()
        val bb5 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb5, 20).await()
        val bb6 = ByteBuffer.wrap(new Array[Byte](sb_aux))
        fileStore.putAtViaSortingBuffer(bb6, 15).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 21)
        assert(fileStore.get(15).await().length === sb_aux)
        fileStore.shrink(0)
        fileStore.resetCounters()

        // insert 10(small), 20(big), 15(exact+1)
        val bb7 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb7, 10).await()
        val bb8 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb8, 20).await()
        val bb9 = ByteBuffer.wrap(new Array[Byte](sb_aux + 1))
        fileStore.putAtViaSortingBuffer(bb9, 15).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 21)
        assert(fileStore.get(15).await().length === (sb_aux + 1))
        fileStore.shrink(0)
        fileStore.resetCounters()
      }

      {
        // insert 10(small), 20(big), 25(small)
        val bb431 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb431, 10).await()
        val bb32 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb32, 20).await()
        val bb33 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb33, 25).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 26)
        fileStore.shrink(0)
        fileStore.resetCounters()

        // insert 10(small), 20(big), 25(exact-1)
        val bb611 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb611, 10).await()
        val bb621 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb621, 20).await()
        val bb631 = ByteBuffer.wrap(new Array[Byte](sb_aux - 1))
        fileStore.putAtViaSortingBuffer(bb631, 25).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 26)
        fileStore.shrink(0)
        fileStore.resetCounters()

        // insert 10(small), 20(big), 25(exact)
        val bb64 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb64, 10).await()
        val bb65 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb65, 20).await()
        val bb66 = ByteBuffer.wrap(new Array[Byte](sb_aux))
        fileStore.putAtViaSortingBuffer(bb66, 25).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 26)
        fileStore.shrink(0)
        fileStore.resetCounters()

        // insert 10(small), 20(big), 15(exact+1)
        val bb67 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb67, 10).await()
        val bb68 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb68, 20).await()
        val bb69 = ByteBuffer.wrap(new Array[Byte](sb_aux + 1))
        fileStore.putAtViaSortingBuffer(bb69, 25).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 26)
        fileStore.shrink(0)
        fileStore.resetCounters()
      }

      {
        // insert 10(small), 20(big), 25(small)
        val bb431 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb431, 10).await()
        val bb32 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb32, 20).await()
        val bb33 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb33, 21).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 22)
        assert(fileStore.get(21).await().length === 10)
        fileStore.shrink(0)
        fileStore.resetCounters()

        // insert 10(small), 20(big), 25(exact-1)
        val bb611 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb611, 10).await()
        val bb621 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb621, 20).await()
        val bb631 = ByteBuffer.wrap(new Array[Byte](sb_aux - 1))
        fileStore.putAtViaSortingBuffer(bb631, 21).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 22)
        assert(fileStore.get(21).await().length === (sb_aux - 1))
        fileStore.shrink(0)
        fileStore.resetCounters()

        // insert 10(small), 20(big), 25(exact)
        val bb64 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb64, 10).await()
        val bb65 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb65, 20).await()
        val bb66 = ByteBuffer.wrap(new Array[Byte](sb_aux))
        fileStore.putAtViaSortingBuffer(bb66, 21).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 22)
        assert(fileStore.get(21).await().length === sb_aux)
        fileStore.shrink(0)
        fileStore.resetCounters()

        // insert 10(small), 20(big), 15(exact+1)
        val bb67 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb67, 10).await()
        val bb68 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb68, 20).await()
        val bb69 = ByteBuffer.wrap(new Array[Byte](sb_aux + 1))
        fileStore.putAtViaSortingBuffer(bb69, 21).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 22)
        assert(fileStore.get(21).await().length === (sb_aux + 1))
        fileStore.shrink(0)
        fileStore.resetCounters()
      }

      {
        // very Big
        val bb67 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb67, 10).await()
        val bb68 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb68, 20).await()
        val bb69 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE * 2))
        fileStore.putAtViaSortingBuffer(bb69, 21).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 22)
        assert(fileStore.get(21).await().length === (SORTING_BUFFER_DATA_SIZE * 2))
        fileStore.shrink(0)
        fileStore.resetCounters()
      }

      {
        val bb67 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb67, 10).await()
        val bb68 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb68, 20).await()
        val bb69 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE * 2))
        fileStore.putAtViaSortingBuffer(bb69, 25).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 26)
        assert(fileStore.get(25).await().length === (SORTING_BUFFER_DATA_SIZE * 2))
        fileStore.shrink(0)
        fileStore.resetCounters()
      }

      {
        val bb67 = ByteBuffer.wrap(new Array[Byte](10))
        fileStore.putAtViaSortingBuffer(bb67, 10).await()
        val bb68 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE - 10))
        fileStore.putAtViaSortingBuffer(bb68, 20).await()
        val bb69 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE * 2))
        fileStore.putAtViaSortingBuffer(bb69, 15).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 21)
        assert(fileStore.get(15).await().length === (SORTING_BUFFER_DATA_SIZE * 2))
        fileStore.shrink(0)
        fileStore.resetCounters()
      }

      {
        val bb69 = ByteBuffer.wrap(new Array[Byte](SORTING_BUFFER_DATA_SIZE * 2))
        fileStore.putAtViaSortingBuffer(bb69, 15).await()
        Thread.sleep(100)
        fileStore.flushObsolete()
        assert(fileStore.size === 16)
        assert(fileStore.get(15).await().length === (SORTING_BUFFER_DATA_SIZE * 2))
        fileStore.shrink(0)
        fileStore.resetCounters()
      }

    }

    "inserting more than SORTING_BUFFER_TOTAL_SLOTS unordereds to get gaps" taggedAs (FileRangeStoreMSBTest, WeirdSMB1) in {
      val fileTest = new File("data/storeSBV2005-SMB")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 10 * slots, 100)

      (1 to maxSlot).map { i ⇒
        val bb = ByteBuffer.wrap(Array(i.toByte))
        fileStore.putAtViaSortingBuffer(bb, i)
      }

      //fileStore.print()
      assert(fileStore.size === 0)

      fileStore.putAtViaSortingBuffer(ByteBuffer.wrap(Array((maxSlot + 1).toByte)), maxSlot + 1)

      //fileStore.print(3000)
      Thread.sleep(300)
      fileStore.flushObsolete()

      assert(fileStore.size === (maxSlot + 2))
      assert(fileStore.get(0).await().toSeq === Nil)
      assert(fileStore.get(666).await().get === 666.toByte)
    }

    s"inserting overhread measuring maxSlot=$maxSlot" taggedAs FileRangeStoreMSBTest in {
      val thisS = maxSlot
      val array = (0 until 32).map(x ⇒ 0.toByte).toArray
      val data0 = (0 until thisS).toList
      val data1 = rnd.shuffle(data0)
      //val data1 = data0.reverse

      val fileTest1 = new File("data/storeSBV2006-SMB")
      fileTest1.delete()
      val fileTest2 = new File("data/storeSBV2007-SMB")
      fileTest2.delete()

      def testWB(n: Int): (Double, Int) = {
        System.gc()
        Thread.sleep(500)
        val fileStore = new FileRangeStoreMSB(fileTest1, 2 * thisS, 5000, true)
        val t0 = System.nanoTime()
        var cntErr = 0
        Future.sequence(data1.map { i ⇒
          val bb = ByteBuffer.wrap(array)
          Try(fileStore.putAtViaSortingBuffer(bb, i)) match {
            case Success(res) ⇒
              res
            case Failure(ex) ⇒
              if (cntErr == 0) ex.printStackTrace()
              cntErr += 1
              Future.successful()
          }

        }).await()
        assert(cntErr === 0)
        val t1 = System.nanoTime()
        assert(fileStore.size === thisS)
        ((t1 - t0).toDouble / 1e6, cntErr)
      }

      def testDirect(n: Int): (Double, Int) = {
        System.gc()
        Thread.sleep(500)
        val fileStore = new FileRangeStoreMSB(fileTest2, 2 * thisS, 5000, true)
        val t2 = System.nanoTime()
        var cntErr = 0
        data0.foreach { i ⇒
          val bb = ByteBuffer.wrap(array)
          Try(fileStore.putAtSync(bb, i)) match {
            case Success(res) ⇒
            case Failure(ex) ⇒
              if (cntErr == 0) ex.printStackTrace()
              cntErr += 1
          }
        }
        assert(cntErr === 0)
        val t3 = System.nanoTime()
        assert(fileStore.size === thisS)
        ((t3 - t2).toDouble / 1e6, cntErr)
      }

      def test(i: Int) = {
        val (t1, e1) = testWB(i)
        val (t2, e2) = testDirect(i)
        println(s"$i. with buffer = $t1 ($e1), direct=$t2 ($e2)")
      }

      // have to wait some time before previos Async IO is over.
      Thread.sleep(5000)

      for (i ← 1 to 10) test(i)
    }

    s"ignore never happen with SM and prefixFlushes only" taggedAs FileRangeStoreMSBTest in {
      val thisS = maxSlot * 100
      val data = (0 until thisS).grouped(100).flatMap(rnd.shuffle(_)).toSeq
      val array = (0 until 32).map(_.toByte).toArray
      System.gc()
      val fileTest = new File("data/storeSM-SB-0001")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * thisS, 5000, true)

      def testWB(): Unit = {
        fileStore.resetCounters()
        fileStore.shrink(0)
        var cntErr = 0
        data.foreach { i ⇒
          val bb = ByteBuffer.wrap(array)
          Try(fileStore.putAtViaSortingBufferSilent(bb, i).await()) match {
            case Success(res) ⇒
            case Failure(ex) ⇒
              if (cntErr == 0) ex.printStackTrace()
              cntErr += 1
          }
        }
        assert(cntErr === 0)
        assert(fileStore.getCountStats.ignoreCount === 0)
        assert(fileStore.size === data.size)
      }

      for (i ← 1 to 5) testWB()
    }

    s"heavy inserting, maxSlot=$maxSlot" taggedAs HeavyTemporarySMBTest in {
      val thisS = maxSlot * 100
      println(s"prepating data for $maxSlot elements")
      var t0 = System.nanoTime()

      val data = (0 until thisS).grouped(maxSlot / 5).flatMap(rnd.shuffle(_)).toSeq

      var t1 = System.nanoTime()
      println(s"data generated in ${(t1 - t0) / 1e9} sec")

      val arrays = (5 to 100).map(x ⇒ (5 to 100).take(x).map(_.toByte).toArray).toList

      t0 = System.nanoTime()
      System.gc()
      t1 = System.nanoTime()
      println(s"System.gc() in ${(t1 - t0) / 1e9} sec")

      t0 = System.nanoTime()
      val fileTest = new File("data/storeSBVT2006-SMB")
      fileTest.delete()
      t1 = System.nanoTime()
      println(s"fileTest.delete in ${(t1 - t0) / 1e9} sec")

      t0 = System.nanoTime()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * thisS, 5000, true)
      t1 = System.nanoTime()
      println(s"fileStore creating in ${(t1 - t0) / 1e9} sec")

      def testWB(): Double = {
        var cntErr = 0
        val t0 = System.nanoTime()

        var row = 0
        val arrayssize = arrays.size
        data.foreach { i ⇒
          val array = arrays(rnd.nextInt(arrayssize))
          val bb = ByteBuffer.wrap(array)

          Try(fileStore.putAtViaSortingBufferSilent(bb, i).await()) match {
            case Success(res) ⇒
            case Failure(ex) ⇒
              if (cntErr == 0) ex.printStackTrace()
              cntErr += 1
          }
        }
        assert(cntErr === 0)
        val t1 = System.nanoTime()
        t1 - t0
      }

      def test(i: Int) = {
        fileStore.resetCounters()
        fileStore.shrink(0)
        val t1 = testWB() / 1e6
        val stats = fileStore.getCountStats
        println(s"$i. with buffer = $t1, stats=$stats, cnt=${data.size}")
      }

      for (i ← 1 to 60) test(i)
    }

    s"weird case 4" taggedAs FileRangeStoreMSBTest in {
      val thisS = maxSlot * 5
      var t0 = System.nanoTime()
      val data = (0 until thisS).grouped(maxSlot / 5).flatMap(rnd.shuffle(_)).toSeq
      val arrays = (5 to 100).map(x ⇒ (5 to 100).take(x).map(_.toByte).toArray).toList
      System.gc()
      val fileTest = new File("data/storeSBVT2006-SMB")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * thisS, 5000, true)

      def testWB(): Unit = {
        var cntErr = 0
        val arrayssize = arrays.size
        var idx = 0
        data.foreach { i ⇒
          val array = arrays(rnd.nextInt(arrayssize))
          val bb = ByteBuffer.wrap(array)
          if (cntErr == 0) {
            Try(fileStore.putAtViaSortingBufferSilent(bb, i).await()) match {
              case Success(res) ⇒
              case Failure(ex) ⇒
                if (cntErr == 0) ex.printStackTrace()
                cntErr += 1
            }
          }
          idx += 1
        }
        assert(cntErr === 0)
      }

      testWB()
    }

    s"weird case 3" taggedAs (FileRangeStoreMSBTest, WeirdSMB2) in {
      val thisS = maxSlot * 100
      println(s"prepating data for $maxSlot elements - weird case 3")
      var t0 = System.nanoTime()

      rnd.setSeed(1)

      val data = (0 until thisS).grouped(maxSlot - 2).flatMap(rnd.shuffle(_)).toSeq

      var t1 = System.nanoTime()
      println(s"data generated in ${(t1 - t0) / 1e9} sec")

      //val array = (0 until 32).map(_.toByte).toArray
      val arrays = (5 to 100).map(x ⇒ (5 to 100).take(x).map(_.toByte).toArray).toList

      t0 = System.nanoTime()
      System.gc()
      t1 = System.nanoTime()
      println(s"System.gc() in ${(t1 - t0) / 1e9} sec")

      t0 = System.nanoTime()
      val fileTest = new File("data/storeSBVT202406-SMB")
      fileTest.delete()
      t1 = System.nanoTime()
      println(s"fileTest.delete in ${(t1 - t0) / 1e9} sec")

      t0 = System.nanoTime()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * thisS, 500000, true)
      t1 = System.nanoTime()
      println(s"fileStore creating in ${(t1 - t0) / 1e9} sec")

      def testWB(): Double = {
        var cntErr = 0
        val t0 = System.nanoTime()
        var row = 0
        println("writing")
        var cnt = 0

        data.foreach { i ⇒
          val array = arrays(rnd.nextInt(arrays.size))
          val bb = ByteBuffer.wrap(array)

          //if (cnt % 10000 == 0) println(s"cnt=$cnt")
          cnt += 1

          Try(fileStore.putAtViaSortingBufferSilent(bb, i).await()) match {
            case Success(res) ⇒
            case Failure(ex) ⇒
              if (cntErr == 0) ex.printStackTrace()
              cntErr += 1
          }
        }
        assert(cntErr === 0)
        assert(fileStore.size === thisS)

        println("reading")

        // to test case when defragmentation just drop content of buffer
        val zeros = (0 until fileStore.size).count(i ⇒ fileStore.get(i).await().remaining() == 0)
        assert(zeros === fileStore.getCountStats.ignoreCount)

        val t1 = System.nanoTime()
        t1 - t0
      }

      def test(i: Int) = {
        fileStore.resetCounters()
        fileStore.shrink(0)
        val t1 = testWB() / 1e6
        val stats = fileStore.getCountStats
        println(s"$i. with buffer = $t1, stats=$stats, cnt=${data.size}")
      }

      for (i ← 1 to 1) test(i)
    }

    "weird case2" taggedAs FileRangeStoreMSBTest in {
      val fileTest = new File("data/storeSBV200123-SMB")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * slots, 5000)

      rewindBBs()
      var idx = 0
      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          fileStore.putAtViaSortingBuffer(bb, slot)
          //println(s"*$idx* wm=${fileStore.getReadWatermark}, stats=${fileStore.getCountStats}, sb_free_slot=${fileStore.sb_free_slot.mkString("[", ",", "]")}")
          //println(s"*$idx* stats=${fileStore.getCountStats}, getFreeSpace=${fileStore.sb_free_space.getFreeSpace.mkString("[", ",", "]")}")
          idx += 1
        //println(" -> ", slot, bb.remaining(), bb.toSeq.mkString("[", ", ", "]"))
        //fileStore.print()
      }

      //fileStore.print()

      rewindBBs()

      //assert(fileStore.get(7278).await().toSeq === buffers(7278).toSeq)

      shuffleBuffers.foreach {
        case (bb, slot) ⇒
          val bb0 = fileStore.get(slot).await()
          assert((bb0.toSeq, slot) === (bb.toSeq, slot))
          fileStore.releaseBuffer(bb0)

      }
    }

    "weird case1" taggedAs FileRangeStoreMSBTest in {
      val array = (0 until 32).map(_.toByte).toArray
      val data0 = (0 until maxSlot).toList

      val fileTest1 = new File("data/storeSBVW200612-SMB")
      fileTest1.delete()
      val raf = new RandomAccessFile(fileTest1.getPath, "rw")
      raf.setLength(5 * (1 << 20))

      val fileStore = new FileRangeStoreMSB(fileTest1, 2 * maxSlot, 5000, true)
      //fileStore.print()

      def testDirect(n: Int) = {
        System.gc()
        Thread.sleep(100)

        //fileStore.print()

        fileStore.shrink(0)

        var cntErr = 0

        data0.take(1).foreach { i ⇒
          val bb = ByteBuffer.wrap(array)
          Try(fileStore.putAtSync(bb, i)) match {
            case Success(_) ⇒
            case Failure(ex) ⇒
              if (cntErr == 0) ex.printStackTrace()
              cntErr += 1
          }
        }
        assert(cntErr === 0)
      }

      testDirect(0)
      testDirect(1)
    }

    "trash test" taggedAs FileRangeStoreMSBTest in {
      val fileTest = new File("data/storeSBVWTT0001-SMB")
      val fileTestT = new File("data/storeSBVWTT0001_TRASH-SMB")
      fileTest.delete()
      fileTestT.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * maxSlot, 5000, true)
      fileStore.enableTrash(fileTestT)
      fileStore.putAtSync(ByteBuffer.wrap(new Array[Byte](10)).put(1.toByte), 10)

      val bb1 = ByteBuffer.allocate(3).put(9.toByte).put(9.toByte).put(9.toByte)
      bb1.flip()
      fileStore.putAtViaSortingBufferSilent(bb1, 9).await()
      val bb2 = ByteBuffer.allocate(1).put(8.toByte)
      bb2.flip()
      fileStore.putAtViaSortingBufferSilent(bb2, 8).await()
      val bb3 = ByteBuffer.allocate(2).put(7.toByte).put(7.toByte)
      bb3.flip()
      fileStore.putAtViaSortingBufferSilent(bb3, 7).await()

      val data = (for (tm ← fileStore.readTrashMeta.take(100)) yield {
        val bb = fileStore.readTrashBufferSync(tm)
        (tm, bb.toSeq)
      }).toList
      assert(data(0) === (TrashMeta(9, 3, FileRangeStore.TRASH_RESERVED + 8), Seq(9.toByte, 9.toByte, 9.toByte)))
      assert(data(1) === (TrashMeta(8, 1, FileRangeStore.TRASH_RESERVED + 16 + 3), Seq(8.toByte)))
      assert(data(2) === (TrashMeta(7, 2, FileRangeStore.TRASH_RESERVED + 24 + 3 + 1), Seq(7.toByte, 7.toByte)))
      assert(data.size === 3)

    }

    "weird case5" taggedAs FileRangeStoreMSBTest in {
      val thisS = maxSlot * 100
      var t0 = System.nanoTime()
      val data = (0 until thisS).grouped(maxSlot / 3).flatMap(rnd.shuffle(_)).toSeq
      //val data = rnd.shuffle((0 until thisS).toList).take(4 * maxSlot).sorted
      val arrays = (5 to (10000, 100)).zipWithIndex.map(x ⇒ 5.to(10000, 100).take(x._2).map(_.toByte).toArray).toList
      System.gc()
      val fileTest = new File("data/storeSBVT2009-S-SMB")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * thisS, 5000, true)

      def testWB(): Unit = {
        var cntErr = 0
        val arrayssize = arrays.size
        var idx = 0
        data.foreach { i ⇒
          if (cntErr == 0) {
            val array = arrays(rnd.nextInt(arrayssize))
            val bb = ByteBuffer.wrap(array)
            if (cntErr == 0) {
              Try(fileStore.putAtViaSortingBufferSilent(bb, i).await()) match {
                case Success(res) ⇒
                case Failure(ex) ⇒
                  if (cntErr == 0) ex.printStackTrace()
                  cntErr += 1
              }
            }
            idx += 1
          }
        }
      }

      testWB()
    }

    "copy allocated test" taggedAs FileRangeStoreMSBTest in {
      // magic number to maximize fragmentation
      val magic1 = 16
      val magic2 = 32764
      val magic3 = 62
      val thisS = maxSlot * magic1
      var t0 = System.nanoTime()
      val data = (0 until thisS).grouped(maxSlot).flatMap(rnd.shuffle(_)).toSeq.toList.take(magic2)
      val arrays = (1 to magic3).map(x ⇒ (0 to x).map(_.toByte).toArray)
      System.gc()
      val fileTest = new File("data/storeSBVT2009-S-SMB")
      fileTest.delete()
      val fileStore = new FileRangeStoreMSB(fileTest, 2 * thisS, 5000, true)

      def testWB(): Unit = {
        var cntErr = 0
        val arrayssize = arrays.size
        var idx = 0
        var idxMax = -1
        var max = 0
        data.foreach { i ⇒
          if (cntErr == 0) {
            val array = arrays(rnd.nextInt(arrayssize))
            val bb = ByteBuffer.wrap(array)
            if (cntErr == 0) {
              Try(fileStore.putAtViaSortingBufferSilent(bb, i).await()) match {
                case Success(res) ⇒
                case Failure(ex) ⇒
                  if (cntErr == 0) ex.printStackTrace()
                  cntErr += 1
              }
            }
            //if (idx % 10000 == 0) println(s"idx=$idx, idxMax=$idxMax, max=$max")
            //println(idx, fileStore.getCountStats)
            idx += 1
          }
        }
        assert(cntErr === 0)
        println(idxMax, max)
      }

      testWB()

      val fileTestT = new File("data/storeSBVT2009-T-SMB")
      fileTestT.delete()
      val cp = fileStore.copyTo(fileTestT)
      assert(cp.size === fileStore.size)
      println("fileStore.getCountStats", fileStore.getCountStats)
      println("fileStore.bufferedSlots.size", fileStore.bufferedSlots.size)
    }

  }
}