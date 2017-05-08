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

  object FileRangeStoreWithSortingBufferV2Test extends Tag("FileRangeStoreWithSortingBufferV2Test")

  object BufferPoolConcurrentTest extends Tag("BufferPoolConcurrentTest")

  object ConcurencyAlgAllTest extends Tag("ConcurencyAlgAllTest")

  object HeavyTemporaryTest extends Tag("HeavyTemporaryTest")

}