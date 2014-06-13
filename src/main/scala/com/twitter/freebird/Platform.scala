package com.twitter.freebird

/**
  * Base trait for freebird compilers.
  */
trait FreePlatform[P <: FreePlatform[P]] extends StreamPlatform[P]

trait StreamPlatform[P <: StreamPlatform[P]] extends Platform[P]

trait Platform[P <: Platform[P]] {
  type Source[_]
  type Store[_]
}

import collection.mutable.{ Map => MMap }

object MemoryPlatform extends FreePlatform[MemoryPlatform] {
  type Source[T] = TraversableOnce[T]
  type Store[T] = MMap[T]
}
