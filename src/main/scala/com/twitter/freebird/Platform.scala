package com.twitter.freebird

/**
  * Base trait for freebird compilers.
  */
trait FreePlatform[P <: FreePlatform[P]] extends StreamPlatform[P]

trait StreamPlatform[P <: StreamPlatform[P]] {
  type Source[_]
  type Store[_]
  type Plan[_]

  def plan[T, S <: PlannableState](p: Producer[MemoryPlatform, S, Id[T]]): Plan[T]

  //TODO can we avoid the id/keyed explosion?
  //def run[T](plan: Plan[T]): Unit
}

import collection.mutable.{ Buffer, Map => MMap }

class MemoryPlatform extends FreePlatform[MemoryPlatform] {
  type Source[T] = List[T]
  type Store[T] = Buffer[T]

  type Plan[T] = MemoryPhysical[T]

  private type MProducer[S <: State, R <: Ret] = Producer[MemoryPlatform, S, R]

  private[this] def optionFun2ConcatFun[U, T](fn: U => Option[T]): U => TraversableOnce[T] = fn(_).toList

  private[this] def map2ConcatFun[U, T](fn: U => T): U => TraversableOnce[T] = { v => List(fn(v)) }

  private[this] def keyGrabbingFun[K, V](fn: V => K): V => (K, V) = { v => (fn(v), v)}

  //TODO we need a way to map from the T in the Producer to the T in MemoryPhysical.
  // we can either leak something into the Producer, that is, the effective type
  // in the underlying, but maintain type safety, or we can try and figure something else out.
  // First try to figure something out, as we want this to be abstract.

  //TODO might need a special one for the keyed ones?
  private[this] def idInPlan[T, S <: State](p: MProducer[S, Id[T]]): MemoryPhysical[T] =
    p match {
      case Source(source) => SourceMP(source)
      case Map(parent, fn) => idInPlan(ConcatMap(parent, map2ConcatFun(fn)))
      case OptionMap(parent, fn) => idInPlan(ConcatMap(parent, optionFun2ConcatFun(fn)))
      case ConcatMap(parent, fn) => ConcatMapMP(Some(idInPlan(parent)), fn, Nil)
      case Filter(parent, fn) => idInPlan(ConcatMap(parent, { v: T => if (fn(v)) List(v) else Nil }))
      case Store(parent, store) => StoreMP(idInPlan(parent), store)
      case Name(parent, name) => idInPlan(parent)
    }

  private[this] def keyedInPlan[K, V, S <: State](p: MProducer[S, Keyed[K, V]]): MemoryPhysical[(K, Seq[V])] =
    p match {
      // TODO we need to deal with the synthetic keyed type.
      // Perhaps we can make the T in producer be a type encapsulating return values?
      // Or perhaps it is enough to have inPlan have an ev parameter for the applicable ones?
      case Group(parent) => GroupMP(idInPlan(parent))
      case GroupBy(parent, fn) => keyedInPlan(Group(Map(parent, keyGrabbingFun(fn))))
      case GroupAll(parent) => keyedInPlan[Unit, V, S](GroupBy(parent, { _ => Unit }))
      case Reducer(parent, fn) => throw new UnsupportedOperationException("implement")
      case Sorted(parent, ord) => throw new UnsupportedOperationException("implement")
      case Fold(parent, init, fn) => throw new UnsupportedOperationException("implement")
      case Name(parent, name) => keyedInPlan(parent)
    }

  //TODO as with store, I don't like that this force Id[T]
  override def plan[T, S <: PlannableState](p: Producer[MemoryPlatform, S, Id[T]]) = idInPlan(p)

  /*override*/ def run[T](plan: MemoryPhysical[T]) {
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
  require(input.nonEmpty, "Source must have input. Otherwise don't create one.")
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
case class ConcatMapMP[T, U](p: Option[MemoryPhysical[T]], fn: T => TraversableOnce[U], rem: List[U])
    extends MemoryPhysical[U] {
  override def getNext =
    rem match {
      case Nil =>
        val (v, newp) = p.getOrElse(throw new IllegalStateException("Nothing from source")).getNext
        val newv = fn(v).toList
        if (newv.isEmpty) {
          ConcatMapMP(newp, fn, Nil).getNext
        } else {
          val head = newv.head
          val tail = newv.tail
          if (newp.isEmpty && tail.isEmpty) {
            (head, None)
          } else {
            (head, Some(ConcatMapMP(newp, fn, tail)))
          }
        }
      case head :: Nil =>
        (head, p.map { par =>
          val (newv, newp) = par.getNext
          ConcatMapMP(newp, fn, fn(newv).toList)
        })
      case head :: tail => (head, Some(ConcatMapMP(p, fn, tail)))
    }
}

case class GroupMP[K, V](p: MemoryPhysical[(K, V)]) extends MemoryPhysical[(K, Seq[V])] {
  override def getNext = throw new UnsupportedOperationException("type checks!")
}
