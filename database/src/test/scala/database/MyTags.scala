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

  object FileRangeStoreMSBTest extends Tag("FileRangeStoreMSBTest")

  object BufferPoolConcurrentTest extends Tag("BufferPoolConcurrentTest")

  object ConcurencyAlgAllTest extends Tag("ConcurencyAlgAllTest")

  object HeavyTemporaryTest extends Tag("HeavyTemporaryTest")

  object HeavyTemporarySMBTest extends Tag("HeavyTemporarySMBTest")

  object WeirdCase1 extends Tag("WeirdCase1")

  object WeirdCase2 extends Tag("WeirdCase2")

  object WeirdCase3 extends Tag("WeirdCase3")

  object WeirdCase4 extends Tag("WeirdCase4")

  object WeirdCase5 extends Tag("WeirdCase5")

  object PerfCase1 extends Tag("PerfCase1")

  object SBFlush extends Tag("SBFlush")

  object SMWBIgnore extends Tag("SMWBIgnore")

  object IntervalsImplTest extends Tag("IntervalsImplTest")

  object IntervalsImplPrintTest extends Tag("IntervalsImplPrintTest")

  object TrashTest extends Tag("TrashTest")

  object DefragmentationTestInterrupt extends Tag("DefragmentationTestInterrupt")

  object CopyAllocatedTest extends Tag("CopyAllocatedTest")

  object SortedStructTest extends Tag("SortedStructTest")

  object WeirdSMB1 extends Tag("WeirdSMB1")

  object WeirdSMB2 extends Tag("WeirdSMB2")

  object WeirdSMB3 extends Tag("WeirdSMB3")

  object FileStoreTest extends Tag("FileStoreTest")

  object FileSequenceTest extends Tag("FileSequenceTest")

}