package com.twitter.freebird

/**
  * Base trait for freebird compilers.
  */
trait FreePlatform[P <: FreePlatform[P]] extends StreamPlatform[P]

trait StreamPlatform[P <: StreamPlatform[P]] {
  type Source[_]
  type Store[_]
  type Plan

  def plan[T, S <: PlannableState](p: Producer[P, S, T]): Plan
  def run(plan: Plan): Unit
}

import collection.mutable.{ Buffer, Map => MMap }

class MemoryPlatform extends FreePlatform[MemoryPlatform] {
  type Source[T] = TraversableOnce[T]
  type Store[T] = Buffer[T]
  type Plan = MemoryPhysical

  override def plan[T, S <: PlannableState](p: Producer[MemoryPlatform, S, T]): MemoryPhysical = {
    null
  }

  override def run(plan: MemoryPhysical) {

  }
}

sealed trait MemoryPhysical
//case class SourceMP(input: TraversableOnce[T])
