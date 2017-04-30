package database

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor._
import BufferUtil._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class FileRangeStoreTest
    extends TestKit(ActorSystem("FileRangeStoreTest"))
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  import system.dispatcher

  val arr1: Array[Byte] = Array(1.toByte, 2.toByte, 3.toByte)
  val arr2: Array[Byte] = Array(4.toByte, 5.toByte, 6.toByte, 7.toByte)
  val arr3: Array[Byte] = Array(8.toByte, 9.toByte, 10.toByte, 11.toByte, 12.toByte)

  val bb1: ByteBuffer = ByteBuffer.wrap(arr1)
  val bb2: ByteBuffer = ByteBuffer.wrap(arr2)
  val bb3: ByteBuffer = ByteBuffer.wrap(arr3)

  def rewindBBs(): Unit = {
    bb1.rewind()
    bb2.rewind()
    bb3.rewind()
  }

  "FileRangeStore tests" must {
    val fileTest1 = new File("data/store001")
    fileTest1.delete()
    val fileStore1 = new FileRangeStore(fileTest1, 123456)

    fileStore1.put(bb1)
    fileStore1.put(bb2)
    fileStore1.put(bb3)

    "fileStore1 of 3 elements" in {
      assert(fileStore1.get(2).await.get() === 8)
      assert(fileStore1.get(1).await.limit() === 4)
      assert(fileStore1.get(1).await.get() === 4)
      assert(fileStore1.get(0).await.limit() === 3)
    }

    fileStore1.commitAll()
    fileStore1.raf.getChannel.close()
    rewindBBs()
    val fileStore2 = new FileRangeStore(fileTest1, 123456)
    fileStore2.put(bb1)
    fileStore2.put(bb2)
    fileStore2.put(bb3)

    "fileStore2 of 3+3 elements" in {
      assert(fileStore2.get(5).await.get() === 8)
      assert(fileStore2.get(4).await.limit() === 4)
      assert(fileStore2.get(4).await.get() === 4)
      assert(fileStore2.get(3).await.limit() === 3)
    }

    "fileStore2 shrink to 3 elements" in {
      fileStore2.shrink(3)
      assert(fileStore2.size === 3)
      assert(fileStore2.get(2).await.get() === 8)
      assert(fileStore2.get(1).await.limit() === 4)
      assert(fileStore2.get(1).await.get() === 4)
      assert(fileStore2.get(0).await.limit() === 3)
    }

    "fileStore2 shrink to 0 elements" in {
      fileStore2.shrink(0)
      assert(fileStore2.size === 0)
      assertThrows[IndexOutOfBoundsException](fileStore2.get(0))
    }

    fileStore2.raf.getChannel.close()
  }

  "FileRangeStore range tests" must {
    rewindBBs()
    val fileTest = new File("data/store002")
    fileTest.delete()
    val fileStore = new FileRangeStore(fileTest, 30)

    fileStore.put(bb1)
    val arrMid = Array(
      20.toByte, 21.toByte, 22.toByte,
      23.toByte, 24.toByte, 25.toByte, 26.toByte, 27.toByte,
      28.toByte, 29.toByte
    )
    val bbMid = ByteBuffer.wrap(arrMid)
    val offsets = Array(3, 8)
    fileStore.putRange(bbMid, offsets)
    fileStore.put(bb2)

    "fileStore test" in {
      assert(fileStore.size === 5)
      assert(fileStore.get(1).await.get() === 20)
      assert(fileStore.get(2).await.get() === 23)
      assert(fileStore.get(3).await.limit === 2)
      assert(fileStore.getRange(1, 3).await.limit === 10)
      assert(fileStore.getRange(1, 3).await.get === 20)
      assert(fileStore.getRange(1, 3).await.toSeq(9) === 29)
      assert(fileStore.getRange(1, 500).await.toSeq.last === 7)
      assert(fileStore.getRange(200, 500).await.toSeq === Nil)
      assert(fileStore.getRange(-1, -2).await.toSeq === Nil)
      assert(fileStore.getRange(-1, 0).await.get() === 1)
      assert(fileStore.getRange(0, 0).await.get() === 1)
      assert(fileStore.getRange(4, 5).await.get() === 4)
    }

    val fileTestCopy = new File("d:/store002")
    fileTestCopy.delete()
    val fileStoreCopy = fileStore.copyTo(fileTestCopy)

    "copy test" in {
      assert(fileStoreCopy.size === 5)
      assert(fileStoreCopy.get(1).await.get() === 20)
      assert(fileStoreCopy.get(2).await.get() === 23)
      assert(fileStoreCopy.get(3).await.limit === 2)
      assert(fileStoreCopy.getRange(1, 3).await.limit === 10)
      assert(fileStoreCopy.getRange(1, 3).await.get === 20)
      assert(fileStoreCopy.getRange(1, 3).await.toSeq(9) === 29)
      assert(fileStoreCopy.getRange(1, 500).await.toSeq.last === 7)
      assert(fileStoreCopy.getRange(200, 500).await.toSeq === Nil)
      assert(fileStoreCopy.getRange(-1, -2).await.toSeq === Nil)
      assert(fileStoreCopy.getRange(-1, 0).await.get() === 1)
      assert(fileStoreCopy.getRange(0, 0).await.get() === 1)
      assert(fileStoreCopy.getRange(4, 5).await.get() === 4)
    }

  }

  "FileRangeStore big test" must {
    val buffSize = 4 * 1024
    val floodSize = FileRangeStore.MAX_POS / buffSize
    val arr0 = new Array[Byte](buffSize)
    for (i ← arr0.indices) arr0(i) = i.toByte
    val bb = ByteBuffer.wrap(arr0)
    val fileTest = new File("d:/store003")
    fileTest.delete()
    val fileStore = new FileRangeStore(fileTest, floodSize)

    "failed to save > MAX_POS" in {
      fileStore.shrink(0)

      assertThrows[IndexOutOfBoundsException] {
        for (i ← 1 to floodSize) {
          bb.rewind()
          fileStore.putSync(bb)
        }
      }
    }

    "try to flood queue" in {
      fileStore.shrink(0)
      fileStore.maxQueueSize = 0

      var loadActive = true
      val parallel = 4
      for (i ← 1 to parallel) Future {
        val arr1024 = new Array[Byte](1024)
        for (i ← arr1024.indices) arr1024(i) = i.toByte
        while (loadActive) {
          val loadFile = new File(s"d:/128Mb.$i")
          loadFile.delete()
          val raf = new RandomAccessFile(loadFile, "rw")
          for (i ← 1 to 128 * 1024) raf.write(arr1024)
          raf.close()
        }
      }

      // try to get long queue
      val res = Future.sequence(for (i ← 1 to 40000) yield {
        bb.rewind()
        Future(fileStore.putAsync(bb))
      })

      Await.ready(res, 300 seconds)
      loadActive = false

      if (fileStore.maxQueueSize >= FileRangeStore.MAX_WRITE_QUEUE) {
        assertThrows[FileRangeStore.WriteQueueOverflowException](res.value.get.get)
      } else {
        println(s"Queue has not exceeded the limit during the test: maxQueueSize=${fileStore.maxQueueSize}, MAX_WRITE_QUEUE=${FileRangeStore.MAX_WRITE_QUEUE}")
      }
    }
  }

  "FileRangeStore tests" must {
    rewindBBs()
    val fileTest = new File("data/store004")
    fileTest.delete()
    val fileStore = new FileRangeStore(fileTest, 50)

    "putAt empty" in {
      assertThrows[IndexOutOfBoundsException](fileStore.putAt(bb1, -1))
    }

    fileStore.putAt(bb1, 10)
    fileStore.putAt(bb2, 20)
    fileStore.putAt(bb3, 40)

    "putAt test" in {
      assert(fileStore.size === 41)
      assert(fileStore.get(0).await.limit() === 0)
      assert(fileStore.get(9).await.limit() === 0)
      assert(fileStore.get(10).await.limit() === 3)
      assert(fileStore.get(20).await.last === 7)
      assert(fileStore.get(39).await.limit() === 0)
      assert(fileStore.get(40).await.toSeq === Seq(8, 9, 10, 11, 12))
      assert(fileStore.getRange(1, 500).await.toSeq === (1 to 12))
      assert(fileStore.getRange(200, 500).await.toSeq === Nil)
      assertThrows[IndexOutOfBoundsException](fileStore.putAt(bb2, 21))
    }
  }

  override def afterAll(): Unit = {
    shutdown()
  }

}