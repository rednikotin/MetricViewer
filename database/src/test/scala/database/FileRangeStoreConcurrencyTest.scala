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

  val sz = 4096
  //val sz = 1024 * 1024
  //val sz = 128

  "FileRangeStore Concurrency test" must {
    Thread.sleep(5000)
    System.gc()

    val fileTest = new File("data/storeC001")
    //val fileTest = new File("d:/storeC001")
    fileTest.delete()
    val slots = Int.MaxValue / sz
    val fileStore = new FileRangeStore(fileTest, slots)
    val arr = new Array[Byte](sz)
    for (i ← arr.indices) arr(i) = i.toByte
    val after200ms = akka.pattern.after[Unit](200 millis, system.scheduler)(_)
    val cnt = (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz
    var slotCheck = 0
    val iSlotCheck = cnt / 3
    def put(p: Promise[Unit], i: Int): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
      try {
        val bb = ByteBuffer.wrap(arr.clone())
        bb.asIntBuffer().put(0, i)
        val res = fileStore.putAsync(bb)
        if (i == iSlotCheck) slotCheck = res.slot
        p.success()
      } catch {
        case ex: FileRangeStore.WriteQueueOverflowException ⇒
          after200ms(Future(put(p, i)))
        case ex: Throwable ⇒
          p.failure(ex)
          ex.printStackTrace()
      }
    } else {
      after200ms(Future(put(p, i)))
    }
    val t0 = System.nanoTime()
    val ress = for (i ← 0 until cnt) yield {
      val p = Promise[Unit]()
      Future(put(p, i))
      p.future
    }
    fileStore.commitAll()

    var i = 0
    var notCompleted = true
    var pervNotCompleted = true
    var prevWM = 0
    while (i < 1000 && pervNotCompleted) {
      Thread.sleep(500)
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
    val t1 = System.nanoTime()

    println(s"saved $cnt of $sz in ${(t1 - t0).toDouble / 1000000000} seconds")

    "look Ok after concurrent inserting" in {
      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      val t0 = System.nanoTime()
      val res: Seq[(Int, Int)] = Future.sequence(for (i ← 0 until cnt) yield {
        fileStore.get(i).map(bb ⇒ (i, bb.asIntBuffer().get(0)))
      }).await(10 minutes)
      val t1 = System.nanoTime()
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1000000000} seconds")

      val distinct = res.map(_._2).distinct.size
      assert(distinct === cnt)
      assert(fileStore.get(slotCheck).await().asIntBuffer().get(0) === iSlotCheck)
    }

  }

  "FileRangeStore MMAP/Concurrency test" must {
    Thread.sleep(5000)
    System.gc()

    val fileTest = new File("data/storeC002")
    //val fileTest = new File("d:/storeC002")
    fileTest.delete()
    //val sz = 128
    val slots = Int.MaxValue / sz
    val fileStore = new FileRangeStore(fileTest, slots)
    fileStore.enableMmapWrite()
    val arr = new Array[Byte](sz)
    for (i ← arr.indices) arr(i) = i.toByte
    val after200ms = akka.pattern.after[Unit](200 millis, system.scheduler)(_)
    val cnt = (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz
    var slotCheck = 0
    val iSlotCheck = cnt / 3
    def put(p: Promise[Unit], i: Int): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
      try {
        val bb = ByteBuffer.wrap(arr.clone())
        bb.asIntBuffer().put(0, i)
        val res = fileStore.putSyncMmap(bb)
        if (i == iSlotCheck) slotCheck = res.slot
        p.success()
      } catch {
        case ex: FileRangeStore.WriteQueueOverflowException ⇒
          after200ms(Future(put(p, i)))
        case ex: Throwable ⇒
          p.failure(ex)
          ex.printStackTrace()
      }
    } else {
      after200ms(Future(put(p, i)))
    }
    val t0 = System.nanoTime()
    val ress = for (i ← 0 until cnt) yield {
      val p = Promise[Unit]()
      Future(put(p, i))
      //if (i % 10000 == 0) println(s"$i / $cnt")
      //put(p, i)
      p.future
    }
    fileStore.commitAll()

    var i = 0
    var notCompleted = true
    var pervNotCompleted = true
    var prevWM = 0
    while (i < 1000 && pervNotCompleted) {
      Thread.sleep(500)
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
    val t1 = System.nanoTime()

    println(s"saved $cnt of $sz in ${(t1 - t0).toDouble / 1000000000} seconds")

    def fmt(x: Long): String = f"${x.toDouble / 1000000}%04.4f"
    def report(c: Seq[Long]) = {
      val alltmp = c.synchronized(c.toList).sorted
      val sz = alltmp.size - 1
      if (sz >= 0) {
        val min = fmt(alltmp(0))
        val mean = fmt(alltmp(sz / 2))
        val p1 = fmt(alltmp(sz / 100))
        val p10 = fmt(alltmp(sz / 10))
        val p25 = fmt(alltmp(sz / 4))
        val p75 = fmt(alltmp(sz * 3 / 4))
        val p90 = fmt(alltmp(sz * 9 / 10))
        val p99 = fmt(alltmp(sz * 99 / 100))
        val max = fmt(alltmp(sz))
        println(s"cnt=${sz + 1}; $min->$p1->$p10->$p25->[$mean]->$p75->$p90->$p99->$max millis")
      } else {
        println("no data")
      }
    }

    "look Ok after concurrent inserting" in {
      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      val t0 = System.nanoTime()
      val res: Seq[(Int, Int, Long)] = Future.sequence(for (i ← scala.util.Random.shuffle((0 until cnt).toList)) yield {
        val t0 = System.nanoTime()
        fileStore.get(i).map { bb ⇒
          val t1 = System.nanoTime()
          (i, bb.asIntBuffer().get(0), t1 - t0)
        }
      }).await(10 minutes)
      val t1 = System.nanoTime()

      report(res.map(_._3))
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1000000000} seconds")

      val distinct = res.map(_._2).distinct.size
      assert(distinct === cnt)
      assert(fileStore.get(slotCheck).await().asIntBuffer().get(0) === iSlotCheck)
    }

  }

}