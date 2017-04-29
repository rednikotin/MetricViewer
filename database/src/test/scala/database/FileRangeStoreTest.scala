package database

import java.io.File
import java.nio.ByteBuffer

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor._
import BufferUtil._

import scala.concurrent.Future

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
  }


  // todo: more concurrent test!!
  "FileRangeStore flood test" must {
    val bb = ByteBuffer.wrap(new Array[Byte](4 * 1024))
    println(bb)
    val fileTest = new File("d:/store003")
    fileTest.delete()
    val fileStore = new FileRangeStore(fileTest, 50000)

    var max = 0
    "99 wites" in {
      for (i ← 1 to 50000) {
        bb.rewind()
        fileStore.putAsync(bb)
        val xxx = fileStore.queueSize
        if (xxx > max) {
          max = xxx
          println(s"queueSize=${xxx}")
        }
      }

      for (i ← 1 to 10) {
        Thread.sleep(100)
        val xxx = fileStore.queueSize
        if (xxx > max) {
          max = xxx
          println(s"queueSize=${xxx}")
        }
      }

      println(max)
    }
  }

  override def afterAll(): Unit = {
    shutdown()
  }

}