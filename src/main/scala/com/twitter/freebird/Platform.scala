package com.twitter.freebird

/**
  * Base trait for freebird compilers.
  */
trait FreePlatform[P <: FreePlatform[P]] extends StreamPlatform[P]

trait StreamPlatform[P <: StreamPlatform[P]] {
  type Source[_]
  type Store[_]
  type Plan[_]

  def plan[T, S <: PlannableState, This <: Producer[P, S, T, This]](p: Producer[P, S, T, This]): Plan[T]

  //TODO can we avoid the id/keyed explosion?
  //def run[T](plan: Plan[T]): Unit
}

import collection.mutable.{ Buffer, Map => MMap }

class MemoryPlatform extends FreePlatform[MemoryPlatform] {
  type Source[T] = List[T]
  type Store[T] = Buffer[T]

  type Plan[T] = MemoryPhysical[T]

  private[this] def optionFun2ConcatFun[U, T](fn: U => Option[T]): U => TraversableOnce[T] = fn(_).toList

  private[this] def map2ConcatFun[U, T](fn: U => T): U => TraversableOnce[T] = { v => List(fn(v)) }

  private[this] def keyGrabbingFun[K, V](fn: V => K): V => (K, V) = { v => (fn(v), v)}

  //TODO I think we can get rid of the State part?
  private[this] def inPlan[T, S <: State, This <: Producer[MemoryPlatform, S, T, This]](p: Producer[MemoryPlatform, S, T, This]): MemoryPhysical[T] =
    p match {
      case Source(source) => SourceMP(source)
      case Map(parent, fn) => inPlan(ConcatMap(parent, map2ConcatFun(fn)))
      case OptionMap(parent, fn) => inPlan(ConcatMap(parent, optionFun2ConcatFun(fn)))
      case ConcatMap(parent, fn) => ConcatMapMP(inPlan(parent), fn)
      case Filter(parent, fn) => inPlan(ConcatMap(parent, { v: T => if (fn(v)) List(v) else Nil }))
      case Group(parent) => GroupMP(inPlan(parent))
      case GroupBy(parent, fn) => inPlan(Group(Map(parent, keyGrabbingFun(fn))))
      case GroupAll(parent) => inPlan(GroupBy(parent, { _ => Unit }))
      case Reducer(parent, fn) => throw new UnsupportedOperationException("implement")
      case Sorted(parent, ord) => throw new UnsupportedOperationException("implement")
      case Fold(parent, init, fn) => throw new UnsupportedOperationException("implement")
      case KeyedWrapper(parent, Name(str)) => inPlan(parent)
      case KeyedWrapper(parent, Store(store)) => StoreMP(inPlan(parent), store)
      case UnkeyedWrapper(parent, Name(str)) => inPlan(parent)
      case UnkeyedWrapper(parent, Store(store)) => StoreMP(inPlan(parent), store)
    }

  //TODO I think we can do _ <: PlannableState, as we don't need it now
  override def plan[T, S <: PlannableState, This <: Producer[MemoryPlatform, S, T, This]](p: Producer[MemoryPlatform, S, T, This]) =
    inPlan[T, S, This](p)

  /*override*/ def run[T](plan: MemoryPhysical[T]) {
    plan.process()
  }
}

//TODO go ahead and just make this get: List[T]
// Screw memory!! It's for testing not efficiency
sealed trait MemoryPhysical[T] {
  def process(): Seq[T]
}
case class SourceMP[T](input: List[T]) extends MemoryPhysical[T] {
  override def process() = input
}
case class StoreMP[T](p: MemoryPhysical[T], buf: Buffer[T]) extends MemoryPhysical[T] {
  override def process() = {
    val data = p.process()
    data.foreach { buf += _ }
    data
  }
}
case class ConcatMapMP[T, U](p: MemoryPhysical[T], fn: T => TraversableOnce[U]) extends MemoryPhysical[U] {
  override def process() = p.process().flatMap(fn)
}

case class GroupMP[K, V](p: MemoryPhysical[(K, V)]) extends MemoryPhysical[(K, TraversableOnce[V])] {
  override def process() = p.process().groupBy(_._1).mapValues(_.map(_._2)).toSeq
}
