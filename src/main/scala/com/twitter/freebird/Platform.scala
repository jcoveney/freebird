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

  private[this] def optionFun2ConcatFun[U, T](fn: U => Option[T]): U => TraversableOnce[T] = fn(_).toList

  private[this] def map2ConcatFun[U, T](fn: U => T): U => TraversableOnce[T] = { v => List(fn(v)) }

  private[this] def keyGrabbingFun[K, V](fn: V => K): V => (K, V) = { v => (fn(v), v)}

  //TODO I think we can get rid of the State part?
  private[this] def inPlan[T, S <: State, This <: Producer[MemoryPlatform, S, T, This]](
    p: Producer[MemoryPlatform, S, T, This]
  ): MemoryPhysical[T] =
    p match {
      case Source(source) => SourceMP(source)
      case Map(parent, fn) => inPlan(parent.concatMap(map2ConcatFun(fn)))
      case OptionMap(parent, fn) => inPlan(parent.concatMap(optionFun2ConcatFun(fn)))
      case ConcatMap(parent, fn) => ConcatMapMP(inPlan(parent), fn)
      case Filter(parent, fn) => inPlan(ConcatMap(parent, { v: T => if (fn(v)) List(v) else Nil }))
      case Group(parent) => GroupMP(inPlan(parent))
      case GroupBy(parent, fn) => inPlan(Group(Map(parent, keyGrabbingFun(fn))))
      case GroupAll(parent) => inPlan(GroupBy(parent, { _ => Unit }))
      case Reducer(parent, fn) => ReducerMP(inPlan(parent), fn)
      case Sorted(parent, ord) => SortMP(inPlan(parent), ord)
      case Fold(parent, init, fn) => FoldMP(inPlan(parent), init, fn)
      case KeyedWrapper(parent, Name(str)) => inPlan(parent)
      case KeyedWrapper(parent, Store(store)) => StoreMP(inPlan(parent), store)
      case UnkeyedWrapper(parent, Name(str)) => inPlan(parent)
      case UnkeyedWrapper(parent, Store(store)) => StoreMP(inPlan(parent), store)
      case Join(left, right) => JoinMP(inPlan(left), inPlan(right))
      case Merge(left, right) => MergeMP(inPlan(left), inPlan(right))
      case Flatten(parent) => FlattenMP(inPlan(parent))
    }

  //TODO I think we can do _ <: PlannableState, as we don't need it now
  override def plan[T, This <: Producer[MemoryPlatform, StoreState, T, This]](p: Producer[MemoryPlatform, StoreState, T, This]) =
    inPlan[T, StoreState, This](p)

  override def run[T](plan: MemoryPhysical[T]) {
    plan.process()
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

case class ReducerMP[K, V](p: MemoryPhysical[(K, TraversableOnce[V])], fn: (V, V) => V) extends MemoryPhysical[(K, V)] {
  override def process() = p.process().map { case (k, v) => (k, v.reduce(fn)) }
}

case class SortMP[K, V](p: MemoryPhysical[(K, TraversableOnce[V])], ord: Ordering[V]) extends MemoryPhysical[(K, TraversableOnce[V])] {
  override def process() = p.process().map { case (k, v) => (k, v.toIndexedSeq.sorted(ord)) }
}

case class FoldMP[K, V, U](p: MemoryPhysical[(K, TraversableOnce[V])], init: U, fn: (U, V) => U) extends MemoryPhysical[(K, U)] {
  override def process() = p.process().map { case (k, v) => (k, v.foldLeft(init)(fn)) }
}

case class MergeMP[T, U](left: MemoryPhysical[T], right: MemoryPhysical[U]) extends MemoryPhysical[Either[T, U]] {
  override def process() = left.process().map(Left(_)) ++ right.process().map(Right(_))
}

case class FlattenMP[K, V](p: MemoryPhysical[(K, TraversableOnce[V])]) extends MemoryPhysical[(K, V)] {
  override def process() = p.process().flatMap { case (k, v) => v.map { (k, _) } }
}

case class JoinMP[K, V, V2](
  left: MemoryPhysical[(K, TraversableOnce[V])],
  right: MemoryPhysical[(K, TraversableOnce[V2])]
) extends MemoryPhysical[(K, TraversableOnce[Either[V, V2]])] {
  override def process() = {
    val leftList = left.process().flatMap { case (k, v) => v.map { in => (k, Left[V, V2](in)) } }
    val rightList = right.process().flatMap { case (k, v) => v.map { in => (k, Right[V, V2](in)) } }
    (leftList ++ rightList).groupBy(_._1).mapValues { _.map(_._2) }.toSeq
  }
}
