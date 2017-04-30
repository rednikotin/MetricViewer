package database

// FileRangeStoreConcurrencyTest
import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor._
import BufferUtil._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

class FileRangeStoreConcurrencyTest
    extends TestKit(ActorSystem("FileRangeStoreConcurrencyTest"))
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

  "FileRangeStore Concurrency test" must {
    val fileTest = new File("data/storeC001")
    fileTest.delete()
    val fileStore = new FileRangeStore(fileTest, 1000000)
    val arr = new Array[Byte](4096)
    for (i ← arr.indices) arr(i) = i.toByte
    val after200ms = akka.pattern.after[Unit](200 millis, system.scheduler)(_)
    val cnt = (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / 4096
    def put(p: Promise[Unit]): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
      try {
        fileStore.putAsync(ByteBuffer.wrap(arr))
        p.success()
      } catch {
        case ex: FileRangeStore.WriteQueueOverflowException ⇒
          after200ms(Future(put(p)))
        case ex: Throwable ⇒
          p.failure(ex)
          ex.printStackTrace()
      }
    } else {
      after200ms(Future(put(p)))
    }
    val ress = for (i ← 1 to cnt) yield {
      val p = Promise[Unit]()
      Future(put(p))
      p.future
    }
    fileStore.commitAll()

    var i = 0
    var notCompleted = true
    var pervNotCompleted = true
    var prevWM = 0
    while (i < 1000 && pervNotCompleted) {
      Thread.sleep(100)
      val completed = ress.count(_.isCompleted)
      val failed = ress.count(_.value.exists(_.isFailure))
      pervNotCompleted = notCompleted
      notCompleted = cnt - completed > 0
      val wm = fileStore.getReadWatermark
      for (i ← 1 to 1000) {
        val rwgap = fileStore.rwGap
        if (rwgap < 0) {
          println("*" * 30 + s" rwgap = $rwgap !!!")
        }
      }
      println(s"cnt completed = $completed, " +
        s"failed=$failed, " +
        s"queue=${fileStore.queueSize}, " +
        s"left = ${cnt - completed}, " +
        s"maxQueue=${fileStore.maxQueueSize}, " +
        s"rwGap=${fileStore.rwGap}," +
        s"wmstep=${wm - prevWM}")
      prevWM = wm
      i += 1
    }

    "???" in {

    }

  }

}