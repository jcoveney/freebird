package com.twitter.freebird

/**
  * Base trait for freebird compilers.
  */
trait FreePlatform[P <: FreePlatform[P]] extends StreamPlatform[P]

trait StreamPlatform[P <: StreamPlatform[P]] {
  type Source[_]
  type Store[_]
  type Plan[_]
  type Keyed[_, _]
  /*
  Note that I would love to have a type Keyed which would abstract out the implementation type of
  a key/value pair in the producer. The producer would be:
  sealed trait KeyedProducer[P <: StreamPlatform[P], S <: State, K, V]
      extends Producer[P, S, P#Keyed[K, V], KeyedProducer[P, S, K, V]]
  and the MemoryPlatform might have
  type Keyed[K, V] = (K, TraversableOnce[V])
  This would make it very easy to evolve the types flowing through. That said, due to an issue (bug? lack?)
  in scala with implicit resolution and type projections, I cannot get this working yet. See:
  https://gist.github.com/jcoveney/7ccb8fdd085ed9a25ac9
  http://stackoverflow.com/questions/24253087/getting-implicit-resolution-and-type-projection-to-work-probably-need-a-workaro
  If I can figure out how to get that case to work, I can probably get this working.

  One solution is to use implicit macros. Since the macro is so simple and it is mainly just type wrangling, I bet it would
  be "easy" to do.
  */

  def plan[T](p: Producer[P, StoreState, T]): Plan[T]

  def run[T](plan: Plan[T]): Unit
}

import collection.mutable.Buffer

class MemoryPlatform extends FreePlatform[MemoryPlatform] {
  type Source[T] = List[T]
  type Store[T] = Buffer[T]
  type Plan[T] = MemoryPhysical[T]
  type Keyed[K, V] = (K, TraversableOnce[V])

  private[this] def inPlan[T](p: Producer[MemoryPlatform, _ <: State, T]): MemoryPhysical[T] =
    p match {
      case Source(source)         => SourceMP(source)
      case ConcatMap(parent, fn)  => ConcatMapMP(inPlan(parent), fn)
      case Group(parent)          => GroupMP(inPlan(parent))
      case CoGroup(left, right)   => CoGroupMP(inPlan(left), inPlan(right))
      case Merge(left, right)     => MergeMP(inPlan(left), inPlan(right))
      case OptionMap(parent, fn)  => inPlan(parent.concatMap(fn(_).toList))
      case Map(parent, fn)        => inPlan(parent.concatMap { v => List(fn(v)) })
      case Filter(parent, fn)     => inPlan(parent.concatMap { v => if (fn(v)) List(v) else Nil })
      case Collect(parent, fn)    => inPlan(parent.optionMap(fn.lift))
      case GroupBy(parent, fn)    => inPlan(parent.map { v => (fn(v), v) }.group)
      case GroupAll(parent)       => inPlan(parent.groupBy { _ => Unit })
      // Because the distinction between Keyed/Unkeyed disappears in the physical layer,
      // we can conveniently utilize operations in the unkeyed layer. This is not necessaryily
      // the case for all platforms but is fine here.
      case Unkey(parent)          => inPlan(parent)
      case GroupToKeyed(parent)   => inPlan(parent)
      case Flatten(parent)        => inPlan(parent.unkey.concatMap { case (k, v) => v.map { (k, _) } })
      case Keys(parent)           => inPlan(parent.unkey.map(_._1))
      case Values(parent)         => inPlan(parent.unkey.concatMap(_._2))
      case MapValues(parent, fn)  => inPlan(parent.unkey.map { case (k, v) => (k, v.map(fn).toList) })
      case MapGroup(parent, fn)   => inPlan(parent.unkey.map { case (k, v) => (k, fn(k, v)) })
      case Reducer(parent, fn)    => inPlan(parent.unkey.map { case (k, v) => (k, v.reduce(fn))})
      case Fold(parent, init, fn) => inPlan(parent.unkey.map { case (k, v) => (k, v.foldLeft(init)(fn))})
      case Sorted(parent, ord)    => inPlan(parent.unkey.map { case (k, v) => (k, v.toIndexedSeq.sorted(ord))})
      case UnkeyedWrapper(parent, Name(str))    => inPlan(parent)
      case UnkeyedWrapper(parent, Store(store)) => StoreMP(inPlan(parent), store)
      case KeyedWrapper(parent, Name(str))      => inPlan(parent)
      case KeyedWrapper(parent, Store(store))   => StoreMP(inPlan(parent), store)
      case GroupedWrapper(parent, Name(str))    => inPlan(parent)
      case GroupedWrapper(parent, Store(store)) => StoreMP(inPlan(parent), store)
      case Join(left, right) =>
        inPlan(left.cogroup(right).concatMap { case (k, (lft, rght)) =>
          lft.flatMap { l => rght.map { r => (k, (l, r)) } }
        }.group)
    }

  override def plan[T](p: Producer[MemoryPlatform, StoreState, T]) = inPlan(p)

  override def run[T](plan: MemoryPhysical[T]) {
    plan.process()
  }

  // Useful for debugging
  def dump[T](p: Producer[MemoryPlatform, _ <: State, T]) {
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

case class CoGroupMP[K, V, V2](
  left: MemoryPhysical[(K, TraversableOnce[V])],
  right:  MemoryPhysical[(K, TraversableOnce[V2])]
) extends MemoryPhysical[(K, (TraversableOnce[V], TraversableOnce[V2]))] {
  override def process() = {
    val lft = left.process().flatMap { case (k, v) => v.map { e => (k, Left[V, V2](e)) } }
    val rght = right.process().flatMap { case (k, v) => v.map { e => (k, Right[V, V2](e)) } }
    //TODO rename nodes so this sort of thing doesn't happen (aka the scala Map covered up)
    (lft ++ rght).foldLeft(collection.immutable.Map.empty[K, (List[V], List[V2])]) {
      case (cum, (k, Left(v))) => cum + (k -> cum.get(k).map { case (l, r) => (v :: l, r) }.getOrElse((List(v), Nil)))
      case (cum, (k, Right(v))) => cum + (k -> cum.get(k).map { case (l, r) => (l, v :: r) }.getOrElse((Nil, List(v))))
    }.toSeq
  }
}

case class MergeMP[T, U](left: MemoryPhysical[T], right: MemoryPhysical[U]) extends MemoryPhysical[Either[T, U]] {
  override def process() = left.process().map(Left(_)) ++ right.process().map(Right(_))
}
