package database

import java.io.File
import java.nio.ByteBuffer
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor._
import BufferUtil._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.Try
import MyTags._

class FileRangeStoreConcurrencyTest
    extends TestKit(ActorSystem("FileRangeStoreConcurrencyTest"))
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val sz = 4096
  //val sz = 1024 * 1024
  //val sz = 256
  val dir = "data/"
  //val dir = "d:/"
  val smallI = 5000
  val bigI = 5000
  //val pct = 0.1
  val pct = 1

  import system.dispatcher
  val independent = ActorSystem("SomethingElse")

  def after200ms(x: ⇒ Future[Unit]) = akka.pattern.after[Unit](200 millis, system.scheduler)(x)(system.dispatcher)

  def floop[T](interval: ⇒ FiniteDuration, maxDuration: Duration, condition: ⇒ Boolean)(trunc: ⇒ T): Future[Option[T]] = {
    import independent.dispatcher
    val maxTime = if (maxDuration.isFinite()) {
      System.currentTimeMillis() + maxDuration.toMillis
    } else {
      Long.MaxValue
    }
    def internalLoop(): Future[Option[T]] = {
      akka.pattern.after(interval, independent.scheduler)(Future {
        val res = Try(Some(trunc))
        if (!condition || System.currentTimeMillis() > maxTime) {
          Future.fromTry(res)
        } else {
          internalLoop()
        }
      }).flatten
    }
    if (!condition) {
      Future.successful(None)
    } else {
      val res = Try(Some(trunc))
      if (!condition) {
        Future.fromTry(res)
      } else {
        internalLoop()
      }
    }
  }

  def infoReport(fileStore: FileRangeStore, cnt: Int, t0: Long, ress: Seq[Future[Any]], name: String, over: Future[Long], size: Int = sz): Future[Unit] = {
    import independent.dispatcher
    var notCompleted = true
    //var pervNotCompleted = true
    var prevWM = -1
    floop((if (ress.size < cnt) smallI else bigI) millis, 5 minutes, notCompleted) {
      val snap: Seq[Option[Boolean]] = ress.map(x ⇒ x.value.map(_.isFailure))
      val completed = snap.count(_.isDefined)
      val failed = snap.count(_.exists(_ == true))
      val inserted = snap.size
      //pervNotCompleted = notCompleted
      notCompleted = cnt - completed > 0
      val wm = fileStore.getReadWatermark
      for (i ← 1 to 1000) {
        val rwgap = fileStore.rwGap
        if (rwgap < 0) {
          println("*" * 30 + s" rwgap = $rwgap !!!")
        }
      }
      println(s"$name: cnt = $cnt, " +
        s"inserted = $inserted, " +
        s"completed = $completed, " +
        s"failed=$failed, " +
        s"queue=${fileStore.queueSize}, " +
        s"left = ${cnt - completed}, " +
        s"maxQueue=${fileStore.maxQueueSize}, " +
        s"rwGap=${fileStore.rwGap}," +
        s"wmstep=${wm - prevWM}, " +
        s"wm=$wm, " +
        s"rwToSizeGap=${fileStore.size - wm}")
      prevWM = wm
    }.andThen {
      case _ ⇒
        val t1: Long = over.value.map(_.get).getOrElse(System.nanoTime())
        println(s"$name: saved $cnt of $size in ${(t1 - t0).toDouble / 1e9} seconds")
    }.map(_ ⇒ {})
  }

  def pctReport(c: Seq[Long], name: String): Unit = {
    def fmt(x: Long): String = f"${x.toDouble / 1e6}%04.4f"
    val alltmp = c.synchronized(c.toList).sorted
    val sz = alltmp.size - 1
    if (sz >= 0) {
      val min = fmt(alltmp.head)
      val mean = fmt(alltmp(sz / 2))
      val p1 = fmt(alltmp(sz / 100))
      val p10 = fmt(alltmp(sz / 10))
      val p25 = fmt(alltmp(sz / 4))
      val p75 = fmt(alltmp(sz * 3 / 4))
      val p90 = fmt(alltmp(sz * 9 / 10))
      val p99 = fmt(alltmp(sz * 99 / 100))
      val max = fmt(alltmp(sz))
      println(s"cnt=${sz + 1}; [min|$min]->[p1|$p1]->[p10|$p10]->[p25|$p25]->[$mean]->[p75|$p75]->[p90|$p90]->[p99|$p99]->[max|$max] millis")
    } else {
      println(s"$name: no data")
    }
  }

  "FileRangeStore Concurrency test" must {
    "look Ok after concurrent inserting" taggedAs FileRangeStoreConcurrency in {
      System.gc()

      val fileTest = new File(s"${dir}storeC001")
      fileTest.delete()
      val slots = Int.MaxValue / sz
      val fileStore = new FileRangeStore(fileTest, slots)
      val arr = new Array[Byte](sz)
      for (i ← arr.indices) arr(i) = i.toByte
      val cnt: Int = (pct * (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz).toInt
      var slotCheck = 0
      val iSlotCheck = cnt / 3

      def put(p: Promise[Any], i: Int): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
        try {
          val bb = ByteBuffer.wrap(arr.clone())
          bb.asIntBuffer().put(0, i)
          val res = fileStore.putAsync(bb)
          if (i == iSlotCheck) slotCheck = res.slot
          p.completeWith(res.result)
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

      var t0 = System.nanoTime()
      val ress: mutable.Buffer[Future[Any]] = scala.collection.mutable.Buffer.empty
      val over = Promise[Long]()
      val res = infoReport(fileStore, cnt, t0, ress, "putAsync", over.future)
      for (i ← 0 until cnt) {
        val p = Promise[Any]()
        Future(put(p, i))
        ress += p.future
      }
      over.completeWith(Future.sequence(ress).map(x ⇒ System.nanoTime()))
      res.await(5 minutes)
      fileStore.commitAll()

      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      t0 = System.nanoTime()
      val resr: Seq[(Int, Int, Long)] = Future.sequence(for (i ← 0 until cnt) yield {
        val t0 = System.nanoTime()
        fileStore.get(i).map { bb ⇒
          val t1 = System.nanoTime()
          (i, bb.asIntBuffer().get(0), t1 - t0)
        }
      }).await(10 minutes)
      val t1 = System.nanoTime()
      pctReport(resr.map(_._3), "putAsync")
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1e9} seconds")

      val distinct = resr.map(_._2).distinct.size
      assert(distinct === cnt)
      assert(fileStore.get(slotCheck).await().asIntBuffer().get(0) === iSlotCheck)
    }
  }

  "FileRangeStore MMAP/Concurrency test" must {
    "look Ok after concurrent inserting" taggedAs FileRangeStoreMMAPConcurrency in {
      System.gc()

      val fileTest = new File(s"${dir}storeC002")
      fileTest.delete()
      //val sz = 128
      val slots = Int.MaxValue / sz
      val fileStore = new FileRangeStore(fileTest, slots)
      fileStore.enableMmapWrite()
      val arr = new Array[Byte](sz)
      for (i ← arr.indices) arr(i) = i.toByte
      val cnt: Int = (pct * (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz).toInt
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
      var t0 = System.nanoTime()
      val ress: mutable.Buffer[Future[Unit]] = scala.collection.mutable.Buffer.empty
      val over = Promise[Long]()
      val res = infoReport(fileStore, cnt, t0, ress, "putSyncMmap", over.future)
      for (i ← 0 until cnt) {
        val p = Promise[Unit]()
        Future(put(p, i))
        ress += p.future
      }
      over.completeWith(Future.sequence(ress).map(x ⇒ System.nanoTime()))
      res.await(5 minutes)
      fileStore.commitAll()

      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      t0 = System.nanoTime()
      val resr: Seq[(Int, Int, Long)] = Future.sequence(for (i ← scala.util.Random.shuffle((0 until cnt).toList)) yield {
        val t0 = System.nanoTime()
        fileStore.get(i).map { bb ⇒
          val t1 = System.nanoTime()
          (i, bb.asIntBuffer().get(0), t1 - t0)
        }
      }).await(10 minutes)
      val t1 = System.nanoTime()
      pctReport(resr.map(_._3), "putSyncMmap")
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1e9} seconds")

      val distinct = resr.map(_._2).distinct.size
      assert(distinct === cnt)
      assert(fileStore.get(slotCheck).await().asIntBuffer().get(0) === iSlotCheck)
    }
  }

  "FileRangeStore async putRange test" must {
    "look Ok after concurrent range inserting" taggedAs FileRangeStoreAsyncPutRange in {
      System.gc()

      val fileTest = new File(s"${dir}storeC003")
      fileTest.delete()
      val slots = Int.MaxValue / sz
      val fileStore = new FileRangeStore(fileTest, slots)
      val arr = new Array[Byte](sz)
      for (i ← arr.indices) arr(i) = i.toByte
      val cnt: Int = (pct * (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz).toInt
      val rangeSize = 1 << 14
      //val cnt = 256
      //val rangeSize = 16
      val rangesCnt = cnt / rangeSize

      def put(p: Promise[Any], i: Int): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
        try {
          val cap = sz * rangeSize.min(cnt - rangeSize * i)
          if (cap > 0) {
            val bb = ByteBuffer.allocateDirect(cap)
            val buffs = cap / sz
            val startN = i * rangeSize
            for (j ← 0 until buffs) {
              val bb0 = ByteBuffer.wrap(arr.clone())
              bb0.asIntBuffer().put(0, startN + j)
              bb.put(bb0)
            }
            val offsets = (1 to buffs).map(_ * sz).toArray
            bb.flip()
            val res = fileStore.putRangeAsync(bb, offsets)
            //println(s"i=$i, cap=$cap, buffs=$buffs, startN=$startN, offsets=${offsets.toSeq}, res=$res")
            p.completeWith(res.result)
          } else {
            p.success()
          }
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

      var t0 = System.nanoTime()
      val ress: mutable.Buffer[Future[Any]] = scala.collection.mutable.Buffer.empty
      val over = Promise[Long]()
      val res = infoReport(fileStore, rangesCnt + 1, t0, ress, "putRangeAsync", over.future, sz * rangeSize)
      for (i ← 0 to rangesCnt) {
        val p = Promise[Any]()
        Future(put(p, i))
        ress += p.future
      }
      over.completeWith(Future.sequence(ress).map(x ⇒ System.nanoTime()))
      res.await(5 minutes)
      fileStore.commitAll()

      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      t0 = System.nanoTime()
      val resr: Seq[(Int, Int, Long)] = Future.sequence(for (i ← scala.util.Random.shuffle((0 until cnt).toList)) yield {
        val t0 = System.nanoTime()
        fileStore.get(i).map { bb ⇒
          val t1 = System.nanoTime()
          (i, bb.asIntBuffer().get(0), t1 - t0)
        }
      }).await(10 minutes)
      val t1 = System.nanoTime()
      pctReport(resr.map(_._3), "putRangeAsync")
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1e9} seconds")

      val distinct = resr.map(_._2).distinct.size
      assert(distinct === cnt)
    }

  }

  "FileRangeStore putRange MMAP test" must {
    "look Ok after concurrent range inserting" taggedAs FileRangeStoreAsyncPutRange in {
      System.gc()

      val fileTest = new File(s"${dir}storeC005")
      fileTest.delete()
      val slots = Int.MaxValue / sz
      val fileStore = new FileRangeStore(fileTest, slots)
      fileStore.enableMmapWrite()
      val arr = new Array[Byte](sz)
      for (i ← arr.indices) arr(i) = i.toByte
      val cnt: Int = (pct * (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz).toInt
      val rangeSize = 1 << 14
      //val cnt = 256
      //val rangeSize = 16
      val rangesCnt = cnt / rangeSize

      def put(p: Promise[Unit], i: Int): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
        try {
          val cap = sz * rangeSize.min(cnt - rangeSize * i)
          if (cap > 0) {
            val bb = ByteBuffer.allocateDirect(cap)
            val buffs = cap / sz
            val startN = i * rangeSize
            for (j ← 0 until buffs) {
              val bb0 = ByteBuffer.wrap(arr.clone())
              bb0.asIntBuffer().put(0, startN + j)
              bb.put(bb0)
            }
            val offsets = (1 to buffs).map(_ * sz).toArray
            bb.flip()
            fileStore.putRangeSyncMmap(bb, offsets)
            //println(s"i=$i, cap=$cap, buffs=$buffs, startN=$startN, offsets=${offsets.toSeq}, res=$res")
          }
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

      var t0 = System.nanoTime()
      val ress: mutable.Buffer[Future[Unit]] = scala.collection.mutable.Buffer.empty
      val over = Promise[Long]()
      val res = infoReport(fileStore, rangesCnt + 1, t0, ress, "putRangeSyncMmap", over.future, sz * rangeSize)
      for (i ← 0 to rangesCnt) {
        val p = Promise[Unit]()
        Future(put(p, i))
        ress += p.future
      }
      over.completeWith(Future.sequence(ress).map(x ⇒ System.nanoTime()))

      res.await(5 minutes)
      fileStore.commitAll()

      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      t0 = System.nanoTime()
      val resr: Seq[(Int, Int, Long)] = Future.sequence(for (i ← scala.util.Random.shuffle((0 until cnt).toList)) yield {
        val t0 = System.nanoTime()
        fileStore.get(i).map { bb ⇒
          val t1 = System.nanoTime()
          (i, bb.asIntBuffer().get(0), t1 - t0)
        }
      }).await(10 minutes)
      val t1 = System.nanoTime()
      pctReport(resr.map(_._3), "putRangeSyncMmap")
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1e9} seconds")

      val distinct = resr.map(_._2).distinct.size
      assert(distinct === cnt)
    }

  }

  "FileRangeStore async putRangeAt test" must {
    "look Ok after concurrent rangeAt inserting" taggedAs FileRangeStoreAsyncPutRangeAt in {
      System.gc()

      val fileTest = new File(s"${dir}storeC004")
      fileTest.delete()
      val slots = Int.MaxValue / sz
      val fileStore = new FileRangeStore(fileTest, slots)
      val arr = new Array[Byte](sz)
      for (i ← arr.indices) arr(i) = i.toByte
      val cnt: Int = (pct * (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz).toInt
      val rangeSize = 1 << 14
      val rangesCnt = cnt / rangeSize

      def put(p: Promise[Any], i: Int): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
        try {
          val cap = sz * rangeSize.min(cnt - rangeSize * i)
          if (cap > 0) {
            val bb = ByteBuffer.allocateDirect(cap)
            val buffs = cap / sz
            val startN = i * rangeSize
            for (j ← 0 until buffs) {
              val bb0 = ByteBuffer.wrap(arr.clone())
              bb0.asIntBuffer().put(0, startN + j)
              bb.put(bb0)
            }
            val offsets = (1 to buffs).map(_ * sz).toArray
            bb.flip()
            val res = fileStore.putRangeAtAsync(bb, offsets, startN)
            //println(s"i=$i, cap=$cap, buffs=$buffs, startN=$startN, offsets=${offsets.toSeq}, res=$res")
            p.completeWith(res.result)
          } else {
            p.success()
          }
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

      var t0 = System.nanoTime()
      val ress: mutable.Buffer[Future[Any]] = scala.collection.mutable.Buffer.empty
      val over = Promise[Long]()
      val res = infoReport(fileStore, rangesCnt + 1, t0, ress, "putRangeAtAsync", over.future, sz * rangeSize)
      for (i ← 0 to rangesCnt) {
        val p = Promise[Any]()
        put(p, i)
        ress += p.future
      }
      over.completeWith(Future.sequence(ress).map(x ⇒ System.nanoTime()))

      res.await(5 minutes)
      fileStore.commitAll()

      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      t0 = System.nanoTime()
      val resr: Seq[(Int, Int, Long)] = Future.sequence(for (i ← scala.util.Random.shuffle((0 until cnt).toList)) yield {
        val t0 = System.nanoTime()
        fileStore.get(i).map { bb ⇒
          val t1 = System.nanoTime()
          (i, bb.asIntBuffer().get(0), t1 - t0)
        }
      }).await(10 minutes)
      val t1 = System.nanoTime()
      pctReport(resr.map(_._3), "putRangeAtAsync")
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1e9} seconds")

      assert(resr.exists(x ⇒ x._1 != x._2) === false)

      val distinct = resr.map(_._2).distinct.size
      assert(distinct === cnt)

    }

  }

  // *** AsyncInFuture testing!

  "FileRangeStore Concurrency async in future test" must {
    "look Ok after concurrent inserting" taggedAs FileRangeStoreConcurrencyAsyncFuture in {
      //Thread.sleep(5000)
      System.gc()

      val fileTest = new File(s"${dir}storeAC001")
      fileTest.delete()
      val slots = Int.MaxValue / sz
      val fileStore = new FileRangeStore(fileTest, slots)
      val arr = new Array[Byte](sz)
      for (i ← arr.indices) arr(i) = i.toByte
      val cnt: Int = (pct * (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz).toInt
      var slotCheck = 0
      val iSlotCheck = cnt / 3
      def put(p: Promise[Any], i: Int): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
        try {
          val bb = ByteBuffer.wrap(arr.clone())
          bb.asIntBuffer().put(0, i)
          val res = fileStore.putFAsync(bb)
          if (i == iSlotCheck) slotCheck = res.slot
          p.success(res.result)
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

      var t0 = System.nanoTime()
      val ress: mutable.Buffer[Future[Any]] = scala.collection.mutable.Buffer.empty
      val over = Promise[Long]()
      val res = infoReport(fileStore, cnt, t0, ress, "putFAsync", over.future)
      for (i ← 0 until cnt) {
        val p = Promise[Any]()
        Future(put(p, i))
        ress += p.future
      }
      over.completeWith(Future.sequence(ress).map(x ⇒ System.nanoTime()))

      res.await(5 minutes)
      fileStore.commitAll()

      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      t0 = System.nanoTime()
      val resr: Seq[(Int, Int, Long)] = Future.sequence(for (i ← 0 until cnt) yield {
        val t0 = System.nanoTime()
        fileStore.get(i).map { bb ⇒
          val t1 = System.nanoTime()
          (i, bb.asIntBuffer().get(0), t1 - t0)
        }
      }).await(10 minutes)
      val t1 = System.nanoTime()
      pctReport(resr.map(_._3), "putFAsync")
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1e9} seconds")

      val distinct = resr.map(_._2).distinct.size
      assert(distinct === cnt)
      assert(fileStore.get(slotCheck).await().asIntBuffer().get(0) === iSlotCheck)
    }

  }

  "FileRangeStore MMAP/Concurrency async in future test" must {
    "look Ok after concurrent inserting" taggedAs FileRangeStoreMMAPConcurrencyAsyncFuture in {
      System.gc()

      val fileTest = new File(s"${dir}storeAC002")
      fileTest.delete()
      val slots = Int.MaxValue / sz
      val fileStore = new FileRangeStore(fileTest, slots)
      fileStore.enableMmapWrite()
      val arr = new Array[Byte](sz)
      for (i ← arr.indices) arr(i) = i.toByte
      val cnt: Int = (pct * (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz).toInt
      var slotCheck = 0
      val iSlotCheck = cnt / 3
      def put(p: Promise[Any], i: Int): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
        try {
          val bb = ByteBuffer.wrap(arr.clone())
          bb.asIntBuffer().put(0, i)
          val res = fileStore.putFAsyncMmap(bb)
          if (i == iSlotCheck) slotCheck = res.slot
          p.completeWith(res.result)
        } catch {
          case ex: FileRangeStore.WriteQueueOverflowException ⇒
            after200ms(Future(put(p, i)))
          case ex: Throwable ⇒
            p.failure(ex)
            ex.printStackTrace()
        }
      } else {
        if (i > cnt - 30) {
          println(s"i=$i, Sad")
        }
        after200ms(Future(put(p, i)))
      }
      var t0 = System.nanoTime()
      val ress: mutable.Buffer[Future[Any]] = scala.collection.mutable.Buffer.empty
      val over = Promise[Long]()
      val res = infoReport(fileStore, cnt, t0, ress, "putFAsyncMmap", over.future)
      for (i ← 0 until cnt) {
        val p = Promise[Any]()
        Future(put(p, i))
        ress += p.future
      }
      over.completeWith(Future.sequence(ress).map(x ⇒ System.nanoTime()))

      res.await(5 minutes)
      fileStore.commitAll()

      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      t0 = System.nanoTime()
      val resr: Seq[(Int, Int, Long)] = Future.sequence(for (i ← scala.util.Random.shuffle((0 until cnt).toList)) yield {
        val t0 = System.nanoTime()
        fileStore.get(i).map { bb ⇒
          val t1 = System.nanoTime()
          (i, bb.asIntBuffer().get(0), t1 - t0)
        }
      }).await(10 minutes)
      val t1 = System.nanoTime()
      pctReport(resr.map(_._3), "putFAsyncMmap")
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1e9} seconds")

      val distinct = resr.map(_._2).distinct.size
      assert(distinct === cnt)
      assert(fileStore.get(slotCheck).await().asIntBuffer().get(0) === iSlotCheck)
    }
  }

  "FileRangeStore async putRange async in future test" must {
    "look Ok after concurrent range inserting" taggedAs FileRangeStoreAsyncPutRangeAsyncFuture in {
      System.gc()

      val fileTest = new File(s"${dir}storeAC003")
      fileTest.delete()
      val slots = Int.MaxValue / sz
      val fileStore = new FileRangeStore(fileTest, slots)
      val arr = new Array[Byte](sz)
      for (i ← arr.indices) arr(i) = i.toByte
      val cnt: Int = (pct * (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz).toInt
      val rangeSize = 1 << 14
      //val cnt = 256
      //val rangeSize = 16
      val rangesCnt = cnt / rangeSize

      def put(p: Promise[Any], i: Int): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
        try {
          val cap = sz * rangeSize.min(cnt - rangeSize * i)
          if (cap > 0) {
            val bb = ByteBuffer.allocateDirect(cap)
            val buffs = cap / sz
            val startN = i * rangeSize
            for (j ← 0 until buffs) {
              val bb0 = ByteBuffer.wrap(arr.clone())
              bb0.asIntBuffer().put(0, startN + j)
              bb.put(bb0)
            }
            val offsets = (1 to buffs).map(_ * sz).toArray
            bb.flip()
            val res = fileStore.putRangeFAsync(bb, offsets)
            //println(s"i=$i, cap=$cap, buffs=$buffs, startN=$startN, offsets=${offsets.toSeq}, res=$res")
            p.completeWith(res.result)
          } else {
            p.success()
          }
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

      var t0 = System.nanoTime()
      val ress: mutable.Buffer[Future[Any]] = scala.collection.mutable.Buffer.empty
      val over = Promise[Long]()
      val res = infoReport(fileStore, rangesCnt + 1, t0, ress, "putRangeFAsync", over.future, sz * rangeSize)
      for (i ← 0 to rangesCnt) {
        val p = Promise[Any]()
        Future(put(p, i))
        ress += p.future
      }
      over.completeWith(Future.sequence(ress).map(x ⇒ System.nanoTime()))

      res.await(5 minutes)
      fileStore.commitAll()

      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      t0 = System.nanoTime()
      val resr: Seq[(Int, Int, Long)] = Future.sequence(for (i ← scala.util.Random.shuffle((0 until cnt).toList)) yield {
        val t0 = System.nanoTime()
        fileStore.get(i).map { bb ⇒
          val t1 = System.nanoTime()
          (i, bb.asIntBuffer().get(0), t1 - t0)
        }
      }).await(10 minutes)
      val t1 = System.nanoTime()
      pctReport(resr.map(_._3), "putRangeFAsync")
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1e9} seconds")

      val distinct = resr.map(_._2).distinct.size
      assert(distinct === cnt)
    }

  }

  "FileRangeStore async putRange MMAP async in future test" must {
    "look Ok after concurrent range inserting" taggedAs FileRangeStoreAsyncPutRangeAsyncFuture in {
      System.gc()

      val fileTest = new File(s"${dir}storeAC005")
      fileTest.delete()
      val slots = Int.MaxValue / sz
      val fileStore = new FileRangeStore(fileTest, slots)
      fileStore.enableMmapWrite()
      val arr = new Array[Byte](sz)
      for (i ← arr.indices) arr(i) = i.toByte
      val cnt: Int = (pct * (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz).toInt
      val rangeSize = 1 << 14
      //val cnt = 256
      //val rangeSize = 16
      val rangesCnt = cnt / rangeSize

      def put(p: Promise[Any], i: Int): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
        try {
          val cap = sz * rangeSize.min(cnt - rangeSize * i)
          if (cap > 0) {
            val bb = ByteBuffer.allocateDirect(cap)
            val buffs = cap / sz
            val startN = i * rangeSize
            for (j ← 0 until buffs) {
              val bb0 = ByteBuffer.wrap(arr.clone())
              bb0.asIntBuffer().put(0, startN + j)
              bb.put(bb0)
            }
            val offsets = (1 to buffs).map(_ * sz).toArray
            bb.flip()
            val res = fileStore.putRangeFAsyncMmap(bb, offsets)
            //println(s"i=$i, cap=$cap, buffs=$buffs, startN=$startN, offsets=${offsets.toSeq}, res=$res")
            p.completeWith(res.result)
          } else {
            p.success()
          }
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

      var t0 = System.nanoTime()
      val ress: mutable.Buffer[Future[Any]] = scala.collection.mutable.Buffer.empty
      val over = Promise[Long]()
      val res = infoReport(fileStore, rangesCnt + 1, t0, ress, "putRangeFAsyncMmap", over.future, sz * rangeSize)
      for (i ← 0 to rangesCnt) {
        val p = Promise[Any]()
        Future(put(p, i))
        ress += p.future
      }
      over.completeWith(Future.sequence(ress).map(x ⇒ System.nanoTime()))

      res.await(5 minutes)
      fileStore.commitAll()

      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      t0 = System.nanoTime()
      val resr: Seq[(Int, Int, Long)] = Future.sequence(for (i ← scala.util.Random.shuffle((0 until cnt).toList)) yield {
        val t0 = System.nanoTime()
        fileStore.get(i).map { bb ⇒
          val t1 = System.nanoTime()
          (i, bb.asIntBuffer().get(0), t1 - t0)
        }
      }).await(10 minutes)
      val t1 = System.nanoTime()
      pctReport(resr.map(_._3), "putRangeFAsyncMmap")
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1e9} seconds")

      val distinct = resr.map(_._2).distinct.size
      assert(distinct === cnt)
    }

  }

  "FileRangeStore async putRangeAt async in future test" must {
    "look Ok after concurrent rangeAt inserting" taggedAs FileRangeStoreAsyncPutRangeAtAsyncFuture in {
      System.gc()

      val fileTest = new File(s"${dir}storeAC004")
      fileTest.delete()
      val slots = Int.MaxValue / sz
      val fileStore = new FileRangeStore(fileTest, slots)
      val arr = new Array[Byte](sz)
      for (i ← arr.indices) arr(i) = i.toByte
      val cnt: Int = (pct * (FileRangeStore.MAX_POS - fileStore.SLOTS_LIMIT) / sz).toInt
      val rangeSize = 1 << 14
      val rangesCnt = cnt / rangeSize

      def put(p: Promise[Any], i: Int): Unit = if (fileStore.queueSize <= FileRangeStore.MAX_WRITE_QUEUE * 0.9) {
        try {
          val cap = sz * rangeSize.min(cnt - rangeSize * i)
          if (cap > 0) {
            val bb = ByteBuffer.allocateDirect(cap)
            val buffs = cap / sz
            val startN = i * rangeSize
            for (j ← 0 until buffs) {
              val bb0 = ByteBuffer.wrap(arr.clone())
              bb0.asIntBuffer().put(0, startN + j)
              bb.put(bb0)
            }
            val offsets = (1 to buffs).map(_ * sz).toArray
            bb.flip()
            val res = fileStore.putRangeAtFAsync(bb, offsets, startN)
            //println(s"i=$i, cap=$cap, buffs=$buffs, startN=$startN, offsets=${offsets.toSeq}, res=$res")
            p.completeWith(res.result)
          } else {
            p.success()
          }
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

      var t0 = System.nanoTime()
      val ress: mutable.Buffer[Future[Any]] = scala.collection.mutable.Buffer.empty
      val over = Promise[Long]()
      val res = infoReport(fileStore, rangesCnt + 1, t0, ress, "putRangeAtFAsync", over.future, sz * rangeSize)
      for (i ← 0 to rangesCnt) {
        val p = Promise[Any]()
        put(p, i)
        ress += p.future
      }
      over.completeWith(Future.sequence(ress).map(x ⇒ System.nanoTime()))

      res.await(5 minutes)
      fileStore.commitAll()

      assert(fileStore.size === cnt)
      assert(fileStore.get(123).await().size === sz)

      t0 = System.nanoTime()
      val resr: Seq[(Int, Int, Long)] = Future.sequence(for (i ← scala.util.Random.shuffle((0 until cnt).toList)) yield {
        val t0 = System.nanoTime()
        fileStore.get(i).map { bb ⇒
          val t1 = System.nanoTime()
          (i, bb.asIntBuffer().get(0), t1 - t0)
        }
      }).await(10 minutes)
      val t1 = System.nanoTime()
      pctReport(resr.map(_._3), "putRangeAtFAsync")
      println(s"read $cnt of $sz in ${(t1 - t0).toDouble / 1e9} seconds")

      assert(resr.exists(x ⇒ x._1 != x._2) === false)

      val distinct = resr.map(_._2).distinct.size
      assert(distinct === cnt)

    }

  }

}