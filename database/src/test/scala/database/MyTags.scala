package database

import org.scalatest.Tag

object MyTags {

  object BigTest extends Tag("BigTest")

  object FileRangeStoreConcurrency extends Tag("FileRangeStoreConcurrency")

  object FileRangeStoreAsync extends Tag("FileRangeStoreAsync")

  object FileRangeStoreMMAP extends Tag("FileRangeStoreMMAP")

  object FileRangeStoreAsyncPutRange extends Tag("FileRangeStoreAsyncPutRange")

  object FileRangeStorePutRangeMMAP extends Tag("FileRangeStorePutRangeMMAP")

  object FileRangeStoreAsyncPutRangeAt extends Tag("FileRangeStoreAsyncPutRangeAt")

  object FileRangeStoreConcurrencyAsyncFuture extends Tag("FileRangeStoreConcurrencyAsyncFuture")

  object FileRangeStoreMMAPConcurrencyAsyncFuture extends Tag("FileRangeStoreMMAPConcurrencyAsyncFuture")

  object FileRangeStoreAsyncPutRangeAsyncFuture extends Tag("FileRangeStoreAsyncPutRangeAsyncFuture")

  object FileRangeStorePutRangeMMAPAsyncFuture extends Tag("FileRangeStorePutRangeMMAPAsyncFuture")

  object FileRangeStoreAsyncPutRangeAtAsyncFuture extends Tag("FileRangeStoreAsyncPutRangeAtAsyncFuture")

  object FileRangeStoreWithSortingBufferTest extends Tag("FileRangeStoreWithSortingBufferTest")

  object BufferPoolConcurrentTest extends Tag("BufferPoolConcurrentTest")

  object ConcurencyAlgAllTest extends Tag("ConcurencyAlgAllTest")

  object HeavyTemporaryTest extends Tag("HeavyTemporaryTest")

  object WeirdCase1 extends Tag("WeirdCase1")

  object WeirdCase2 extends Tag("WeirdCase2")

  object WeirdCase3 extends Tag("WeirdCase3")

  object PerfCase1 extends Tag("PerfCase1")

  object SBFlush extends Tag("SBFlush")

  object SMWBIgnore extends Tag("SMWBIgnore")

  object IntervalsImplTest extends Tag("IntervalsImplTest")

  object IntervalsImplPrintTest extends Tag("IntervalsImplPrintTest")

}