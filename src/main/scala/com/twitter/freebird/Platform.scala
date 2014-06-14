package com.twitter.freebird

/**
  * Base trait for freebird compilers.
  */
trait FreePlatform[P <: FreePlatform[P]] extends StreamPlatform[P]

trait StreamPlatform[P <: StreamPlatform[P]] {
  type Source[_]
  type Store[_]
  type Plan[_]

  def plan[T, S <: PlannableState](p: Producer[P, S, T]): Plan[T]
  def run[T](plan: Plan[T]): Unit
}

import collection.mutable.{ Buffer, Map => MMap }

class MemoryPlatform extends FreePlatform[MemoryPlatform] {
  type Source[T] = List[T]
  type Store[T] = Buffer[T]
  type Plan[T] = MemoryPhysical[T]

  private type MProducer[S <: State, T] = Producer[MemoryPlatform, S, T]

  private[this] def inPlan[T, S <: State](p: MProducer[S, T]): MemoryPhysical[T] =
    p match {
      case Source(source) => SourceMP(source)
      case Map(parent, fn) => throw new UnsupportedOperationException("implement")
      case OptionMap(parent, fn) => throw new UnsupportedOperationException("implement")
      case ConcatMap(parent, fn) => throw new UnsupportedOperationException("implement")
      case Filter(parent, fn) => throw new UnsupportedOperationException("implement")
      case Group(parent) => throw new UnsupportedOperationException("implement")
      case GroupBy(parent, fn) => throw new UnsupportedOperationException("implement")
      case GroupAll(parent) => throw new UnsupportedOperationException("implement")
      case Reducer(parent, fn) => throw new UnsupportedOperationException("implement")
      case Sorted(parent, ord) => throw new UnsupportedOperationException("implement")
      case Fold(parent, init, fn) => throw new UnsupportedOperationException("implement")
      case Store(parent, store) => StoreMP(inPlan(parent), store)
      case Name(parent, name) => inPlan(parent)
    }

  override def plan[T, S <: PlannableState](p: MProducer[S, T]): MemoryPhysical[T] = inPlan(p)

  override def run[T](plan: MemoryPhysical[T]) {
    @annotation.tailrec
    def runOn[U](p: MemoryPhysical[U]) {
      val (t, newp) = p.getNext
      if (newp.nonEmpty) {
        runOn(newp.get)
      }
    }
    runOn(plan)
  }
}

sealed trait MemoryPhysical[T] {
  def getNext: (T, Option[MemoryPhysical[T]])
}
case class SourceMP[T](input: List[T]) extends MemoryPhysical[T] {
  override def getNext = input match {
    case head :: Nil => (head, None)
    case head :: tail => (head, Some(SourceMP(tail)))
  }
}
case class StoreMP[T](p: MemoryPhysical[T], buf: Buffer[T]) extends MemoryPhysical[T] {
  override def getNext = {
    val (v, newp) = p.getNext
    buf += v
    (v, newp.map(StoreMP(_, buf)))
  }
}
case class FlatMapMP[T, U](p: MemoryPhysical[T], fn: T => TraversableOnce[U], rem: List[U] = Nil) extends MemoryPhysical[U] {
  override def getNext =
    rem match {
      case head :: Nil =>

      case head :: tail => (v, FlatMapMP(p, fn, tail))
    }
}
