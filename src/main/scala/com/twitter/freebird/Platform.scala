package com.twitter.freebird

/**
  * Base trait for freebird compilers.
  */
trait FreePlatform[P <: FreePlatform[P]] extends StreamPlatform[P]

trait StreamPlatform[P <: StreamPlatform[P]] {
  type Source[_]
  type Store[_]
  type Plan[_]

  def plan[T, This <: Producer[P, StoreState, T, This]](p: Producer[P, StoreState, T, This]): Plan[T]

  def run[T](plan: Plan[T]): Unit
}

import collection.mutable.{ Buffer, Map => MMap }

class MemoryPlatform extends FreePlatform[MemoryPlatform] {
  type Source[T] = List[T]
  type Store[T] = Buffer[T]
  type Plan[T] = MemoryPhysical[T]

  private[this] def inPlan[T, This <: Producer[MemoryPlatform, _ <: State, T, This]](
    p: Producer[MemoryPlatform, _ <: State, T, This]
  ): MemoryPhysical[T] =
    p match {
      case Source(source)         => SourceMP(source)
      case ConcatMap(parent, fn)  => ConcatMapMP(inPlan(parent), fn)
      case Map(parent, fn)        => inPlan(parent.concatMap(v => List(fn(v))))
      case OptionMap(parent, fn)  => inPlan(parent.concatMap(fn(_).toList))
      case Filter(parent, fn)     => inPlan(parent.concatMap { v => if (fn(v)) List(v) else Nil })
      case Collect(parent, fn)    => inPlan(parent.optionMap(fn.lift))
      case Group(parent)          => GroupMP(inPlan(parent))
      case GroupBy(parent, fn)    => inPlan(parent.map { v => (fn(v), v) }.group)
      case GroupAll(parent)       => inPlan(parent.groupBy { _ => Unit })
      case Merge(left, right)     => MergeMP(inPlan(left), inPlan(right))
      case Join(left, right) =>
        inPlan((left.flatten ++ right.flatten).map {
          case Left((k, v)) => (k, Left(v))
          case Right((k, v)) => (k, Right(v))
        }.group)
      // Because the distinction between Keyed/Unkeyed disappears in the physical layer,
      // we can conveniently utilize operations in the unkeyed layer. This is not necessaryily
      // the case for all platforms but is fine here.
      case Unkey(parent)          => inPlan(parent)
      case Flatten(parent)        => inPlan(parent.unkey.concatMap { case (k, v) => v.map { (k, _) } })
      case Keys(parent)           => inPlan(parent.unkey.map(_._1))
      case Values(parent)         => inPlan(parent.unkey.concatMap(_._2))
      case MapValues(parent, fn)  => inPlan(parent.unkey.map { case (k, v) => (k, v.map(fn)) } )
      case Reducer(parent, fn)    => inPlan(parent.unkey.map { case (k, v) => (k, v.reduce(fn))})
      case Fold(parent, init, fn) => inPlan(parent.unkey.map { case (k, v) => (k, v.foldLeft(init)(fn))})
      case Sorted(parent, ord)    => inPlan(parent.unkey.map { case (k, v) => (k, v.toIndexedSeq.sorted(ord))})
      case KeyedWrapper(parent, Name(str))      => inPlan(parent)
      case KeyedWrapper(parent, Store(store))   => StoreMP(inPlan(parent), store)
      case UnkeyedWrapper(parent, Name(str))    => inPlan(parent)
      case UnkeyedWrapper(parent, Store(store)) => StoreMP(inPlan(parent), store)

    }

  //TODO I think we can do _ <: PlannableState, as we don't need it now
  override def plan[T, This <: Producer[MemoryPlatform, StoreState, T, This]]
    (p: Producer[MemoryPlatform, StoreState, T, This]) = inPlan(p)

  override def run[T](plan: MemoryPhysical[T]) {
    plan.process()
  }

  // Useful for debugging
  def dump[T, This <: Producer[MemoryPlatform, _ <: State, T, This]]
    (p: Producer[MemoryPlatform, _ <: State, T, This]) {
      inPlan(p).process().foreach(println(_))
  }
}

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

case class MergeMP[T, U](left: MemoryPhysical[T], right: MemoryPhysical[U]) extends MemoryPhysical[Either[T, U]] {
  override def process() = left.process().map(Left(_)) ++ right.process().map(Right(_))
}
