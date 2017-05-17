package database

import java.io.File
import java.nio.ByteBuffer
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor._
import BufferUtil._
import MyTags._

class FileStoreTest
    extends TestKit(ActorSystem("FileRangeStoreTest"))
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val arr1: Array[Byte] = Array(1.toByte, 2.toByte, 3.toByte)
  val arr2: Array[Byte] = Array(4.toByte, 5.toByte, 6.toByte, 7.toByte)
  val arr3: Array[Byte] = Array(8.toByte, 9.toByte, 10.toByte, 11.toByte, 12.toByte)
  val arr4: Array[Byte] = Array(13.toByte, 14.toByte)
  val arr5: Array[Byte] = Array(42.toByte)

  val bb1: ByteBuffer = ByteBuffer.wrap(arr1)
  val bb2: ByteBuffer = ByteBuffer.wrap(arr2)
  val bb3: ByteBuffer = ByteBuffer.wrap(arr3)
  val bb4: ByteBuffer = ByteBuffer.wrap(arr4)
  val bb5: ByteBuffer = ByteBuffer.wrap(arr5)

  def rewindBBs(): Unit = {
    bb1.rewind()
    bb2.rewind()
    bb3.rewind()
    bb4.rewind()
    bb5.rewind()
  }

  "FileStore tests" must {
    "???" taggedAs FileStoreTest in {
      val maxRows = 100
      val fileTest = new File("data/store-FST0001")
      fileTest.delete()
      val fs = new FileStore(fileTest, maxRows, 1 << 20)

      rewindBBs()

      fs.insert(bb1)
      fs.insert(bb2)
      val id3 = fs.insert(bb3)
      val id4 = fs.insert(bb4)
      fs.insert(bb5)

      rewindBBs()

      fs.delete(id3)
      fs.update(id4, bb5)

      assert(fs.iterator().map(x ⇒ x.copy(_2 = x._2.toSeq)).toList === Seq((0, Seq(1, 2, 3)), (1, Seq(4, 5, 6, 7)), (3, Seq(42)), (4, Seq(42))))

      val fs2 = new FileStore(fileTest, 100, 1 << 20)

      assert(fs2.iterator().map(x ⇒ x.copy(_2 = x._2.toSeq)).toList === Seq((0, Seq(1, 2, 3)), (1, Seq(4, 5, 6, 7)), (3, Seq(42)), (4, Seq(42))))

      fs2.truncate()

      assert(fs2.iterator().map(x ⇒ x.copy(_2 = x._2.toSeq)).toList === Nil)

      val id5 = fs2.insert(bb1)

      assert(fs2.select(id5).get.toSeq === bb1.toSeq)

      fs2.truncate()

      for (i ← 0 until maxRows) {
        fs2.insert(ByteBuffer.wrap(Array(i.toByte)))
      }

      assertThrows[FileStore.NoRowsLeftException](fs2.insert(bb2))
      assert(fs2.select(-333) === None)
      assert(fs2.select(maxRows) === None)
      fs2.delete(maxRows / 2)
      assertThrows[FileStore.RowNotExistsException](fs2.delete(maxRows / 2))
      assert(fs2.select(maxRows / 2) === None)
      val superBB = new Array[Byte](1 << 20)
      superBB(0) = -1
      assertThrows[FileStore.NoSpaceLeftException](fs2.insert(ByteBuffer.wrap(superBB)))
    }
  }

  "IndexedFileStore tests" must {
    "simple test" taggedAs FileStoreTest in {
      val maxRows = 100
      val fileTest = new File("data/store-FST0002")
      fileTest.delete()
      val fs = new FileStore(fileTest, maxRows, 1 << 20)
      val ifs = new UIndexedFileStore(fs, buffer ⇒ buffer.get(0))

      rewindBBs()

      ifs.insert(bb1)
      ifs.insert(bb2)
      val id3 = ifs.insert(bb3)
      val id4 = ifs.insert(bb4)
      ifs.insert(bb5)

      rewindBBs()

      ifs.delete(id3)
      ifs.update(id4, ByteBuffer.wrap(Array(99.toByte)))

      assert(ifs.iterator().map(x ⇒ x.copy(_2 = x._2.toSeq)).toList === Seq((0, Seq(1, 2, 3)), (1, Seq(4, 5, 6, 7)), (3, Seq(99)), (4, Seq(42))))

      val fs2 = new FileStore(fileTest, 100, 1 << 20)
      val ifs2 = new UIndexedFileStore(fs2, buffer ⇒ buffer.get(0))

      assert(ifs2.iterator().map(x ⇒ x.copy(_2 = x._2.toSeq)).toList === Seq((0, Seq(1, 2, 3)), (1, Seq(4, 5, 6, 7)), (3, Seq(99)), (4, Seq(42))))

      ifs2.truncate()

      assert(ifs2.iterator().map(x ⇒ x.copy(_2 = x._2.toSeq)).toList === Nil)

      val id5 = ifs2.insert(bb1)

      assert(ifs2.select(id5).get.toSeq === bb1.toSeq)

      ifs2.truncate()

      for (i ← 0 until maxRows) {
        ifs2.insert(ByteBuffer.wrap(Array(i.toByte)))
      }

      assertThrows[UIndexedStore.DuplicatedValueOnIndexException](ifs2.insert(bb2))
      assert(ifs2.select(-333) === None)
      assert(ifs2.select(maxRows) === None)
      ifs2.delete(maxRows / 2)
      assertThrows[FileStore.RowNotExistsException](ifs2.delete(maxRows / 2))
      assert(ifs2.select(maxRows / 2) === None)
      val superBB = new Array[Byte](1 << 20)
      superBB(0) = -1
      assertThrows[FileStore.NoSpaceLeftException](fs2.insert(ByteBuffer.wrap(superBB)))
      assert(ifs2.selectBy(0)(2.toByte).get.toSeq === Seq(2.toByte))
    }

    "double index" taggedAs FileStoreTest in {
      val maxRows = 100
      val fileTest = new File("data/store-FST0003")
      fileTest.delete()
      val fs = new FileStore(fileTest, maxRows, 1 << 20)
      val ifs0 = new UIndexedFileStore(fs, buffer ⇒ buffer.get(0))
      val ifs1 = new UIndexedFileStore(ifs0, buffer ⇒ buffer.get(1))
      val ifs2 = new UIndexedFileStore(ifs1, buffer ⇒ buffer.get(2))
      val ifs3 = new UIndexedFileStore(ifs2, buffer ⇒ buffer.get(3))

      def buf(x: Int*): ByteBuffer = ByteBuffer.wrap(x.map(_.toByte).toArray)

      ifs3.insert(buf(1, 2, 2, 3))
      ifs3.insert(buf(2, 4, 6, 4))
      ifs3.insert(buf(4, 1, 8, 5))
      ifs3.insert(buf(7, 3, 1, 0))

      assert(ifs3.selectBy(0)(0).get.toSeq(0) === 7)
      assert(ifs2.selectBy(0)(8).get.toSeq(0) === 4)
      assert(ifs1.selectBy(0)(4).get.toSeq(0) === 2)
      assert(ifs0.selectBy(0)(1).get.toSeq(3) === 3)

    }

    "multi index" taggedAs FileStoreTest in {
      val maxRows = 100
      val fileTest = new File("data/store-FST0004")
      fileTest.delete()
      val fs = new FileStore(fileTest, maxRows, 1 << 20)
      val ifs = new UIndexedFileStore(fs, _.get(0), _.get(1), _.get(2), _.get(3), _.getInt(0))

      def buf(x: Int*): ByteBuffer = ByteBuffer.wrap(x.map(_.toByte).toArray)

      ifs.insert(buf(1, 2, 2, 3))
      ifs.insert(buf(2, 4, 6, 4))
      ifs.insert(buf(4, 1, 8, 5))
      ifs.insert(buf(7, 3, 1, 0))

      assert(ifs.selectBy(3)(0).get.toSeq(0) === 7)
      assert(ifs.selectBy(2)(8).get.toSeq(0) === 4)
      assert(ifs.selectBy(1)(4).get.toSeq(0) === 2)
      assert(ifs.selectBy(0)(1).get.toSeq(3) === 3)
      //println(buf(1, 2, 2, 3).getInt(0))
      //println((1 << 24) + (2 << 16) + (2 << 8) + 3)
      assert(ifs.selectBy(4)((1 << 24) + (2 << 16) + (2 << 8) + 3).get.toSeq(0) === 1)

    }
  }

  override def afterAll(): Unit = {
    shutdown()
  }

}