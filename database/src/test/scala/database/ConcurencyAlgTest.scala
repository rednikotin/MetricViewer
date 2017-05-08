package database

import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import MyTags._

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Random, Success, Try}

class ConcurencyAlgTest
    extends TestKit(ActorSystem("ConcurencyAlgTest"))
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  import system.dispatcher

  val total = 300000
  val cnt = 8
  val batch: Int = total / cnt
  val maxSize: Int = 4
  val rnd = new Random()
  val tmw = 5000
  val retries = 10

  val dummy = new AtomicInteger(0)

  case class Resource(content: Int) {
    //Thread.sleep(10)
  }

  val arr = new Array[Int](1 << 14)
  val mem = new MemoryBuffer(1 << 14)
  val arr2 = new Array[AtomicInteger](1 << 14)
  for (i ← arr2.indices) arr2(i) = new AtomicInteger(0)

  def action(): Unit = {
    val x = (Thread.currentThread().getId % (1 << 14)).toInt
    //arr(0) = arr(0) + 1
    //val y = mem.get(0)
    //mem.put(x, 1.toByte)
    //val y = arr(x)
    //arr(x) = y + 1
    //arr2(0).incrementAndGet()

    //dummy.incrementAndGet()
    /*var x = 0
    for (i ← 1 to (tmw + rnd.nextInt(tmw))) {
      x += 1
    }
    if (x % 99999999 > 234234234) throw new IllegalArgumentException
    t match {
      case r: Resource ⇒ r.upd(x)
      case _           ⇒
    }*/
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////

  trait Pool {
    def tryGet(): Resource
    def release(bb: Resource): Unit
  }

  private class BPool(maxSize: Int) extends Pool {
    @volatile private final var size = 0
    private val data = new Array[Resource](maxSize)
    def tryGet0(): Resource = {
      if (size == 0) {
        null
      } else {
        synchronized {
          if (size == 0) {
            null
          } else {
            size -= 1
            data(size)
          }
        }
      }
    }
    def tryGet(): Resource = {
      var res = tryGet0()
      var i = 0
      while (res == null /*&& i < retries*/ ) {
        Thread.`yield`()
        i += 1
        res = tryGet0()
      }
      res
    }
    def release(bb: Resource): Unit = this.synchronized {
      if (size < maxSize) {
        data(size) = bb
        size += 1
      }
    }
  }

  private class NBPool(maxSize: Int) extends Pool {
    assert((maxSize & (maxSize - 1)) == 0)
    private final val modulo = maxSize - 1
    private final val head = new AtomicInteger(0)
    private final val tail = new AtomicInteger(0)
    private final val data = new Array[Resource](maxSize)

    private def tryGet0(): Resource = {
      // order makes sense!!
      val curTail = tail.get
      val curHead = head.get
      val nextTail = curTail + 1
      if ((curTail & modulo) != (curHead & modulo)) {
        val res = data(nextTail & modulo)
        if (tail.compareAndSet(curTail, nextTail)) {
          res
        } else null
      } else null
    }

    def tryGet(): Resource = {
      var res = tryGet0()
      var i = 0
      while (res == null /*&& i < retries*/ ) {
        Thread.`yield`()
        i += 1
        res = tryGet0()
      }
      res
    }

    def release(bb: Resource): Unit = {
      val curTail = tail.get
      val curHead = head.get
      val nextHead = curHead + 1
      if ((nextHead & modulo) != (curTail & modulo)) {
        data(nextHead & modulo) = bb
        head.compareAndSet(curHead, nextHead)
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def poolCheck(pool: Pool, default: ⇒ Resource): Boolean = {
    var miss = false
    var t = pool.tryGet()
    if (t == null) {
      miss = true
      t = default
    }
    action()
    pool.release(t)
    miss
  }

  def bpool(): () ⇒ () ⇒ Unit = () ⇒ {
    val pool = new BPool(maxSize)
    for (i ← 1 to maxSize) pool.release(Resource(0))
    missCnt.set(0)
    () ⇒ {
      if (poolCheck(pool, Resource(0))) missCnt.incrementAndGet()
    }
  }

  def nbpool(): () ⇒ () ⇒ Unit = () ⇒ {
    val pool = new NBPool(maxSize)
    for (i ← 1 to maxSize) pool.release(Resource(0))
    missCnt.set(0)
    () ⇒ {
      if (poolCheck(pool, Resource(0))) missCnt.incrementAndGet()
    }
  }

  def nothing(): () ⇒ () ⇒ Unit = () ⇒ () ⇒ {}
  def actionOnly(): () ⇒ () ⇒ Unit = () ⇒ () ⇒ action()

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////

  case class TestStat(time: Long, totalCnt: Int, doneCnt: Int, errCnt: Int)
  val missCnt = new AtomicInteger(0)

  val test0: (() ⇒ Unit) ⇒ TestStat = (action: () ⇒ Unit) ⇒ {
    val errCnt = new AtomicInteger(0)
    val completes = new Array[Long](cnt)

    val t0 = System.nanoTime()
    var j = 0
    while (j < cnt) {
      val thisJ = j
      Future {
        for (b ← 1 to batch) {
          Try(action()) match {
            case Success(_) ⇒
            case Failure(ex) ⇒
              val n = errCnt.incrementAndGet()
              if (n == 0) ex.printStackTrace()
          }
        }
        val t1 = System.nanoTime()
        completes(thisJ) = t1
      }
      j += 1
    }

    var i = 0
    var done = false
    while (!done && i < 1000) {
      Thread.sleep(100)
      done = completes.forall(_ > t0)
      i += 1
    }
    TestStat(completes.max - t0, cnt, completes.count(_ < t0), errCnt.get)
  }

  def runTest(tests: (String, () ⇒ () ⇒ Unit)*): Unit = {
    val stats = tests.map(x ⇒ (x._1, ListBuffer.empty[Long])).toMap
    val testf = test0
    for {
      (name, setupContext) ← tests
      i ← 1 to 20
    } {
      System.gc()
      Thread.sleep(100)
      rnd.setSeed(0)
      missCnt.set(0)
      val ctx = setupContext()
      val stat = testf(ctx)
      stats(name) += stat.time
      println(s"$name time: ${stat.time / 1e9}, stat=$stat, missCnt=${missCnt.get}")
    }
    stats.foreach {
      case (n, ts) ⇒
        val avg = ts.sum / ts.size
        val mean = ts.sorted.apply(ts.size / 2)
        println(s"$n: ${mean / 1e9}, ${avg / 1e9}")
    }
  }

  "ConcurencyAlgTest" must {
    "test be good" taggedAs ConcurencyAlgAllTest in {
      runTest(
        ("Nonthig   ", nothing()),
        ("ActionOnly", actionOnly()),
        ("BPool     ", bpool()),
        ("NBPool    ", nbpool())
      )
    }
  }

}
