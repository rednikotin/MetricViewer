package database

import org.scalatest.Tag

object MyTags {

  object BigTest extends Tag("BigTest")

  object FileRangeStoreConcurrency extends Tag("FileRangeStoreConcurrency")

  object FileRangeStoreMMAPConcurrency extends Tag("FileRangeStoreMMAPConcurrency")

  object FileRangeStoreAsyncPutRange extends Tag("FileRangeStoreAsyncPutRange")

  object FileRangeStoreAsyncPutRangeAt extends Tag("FileRangeStoreAsyncPutRangeAt")

  object FileRangeStoreConcurrencyAsyncFuture extends Tag("FileRangeStoreConcurrencyAsyncFuture")

  object FileRangeStoreMMAPConcurrencyAsyncFuture extends Tag("FileRangeStoreMMAPConcurrencyAsyncFuture")

  object FileRangeStoreAsyncPutRangeAsyncFuture extends Tag("FileRangeStoreAsyncPutRangeAsyncFuture")

  object FileRangeStoreAsyncPutRangeAtAsyncFuture extends Tag("FileRangeStoreAsyncPutRangeAtAsyncFuture")

}