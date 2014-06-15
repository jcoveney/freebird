package com.twitter.freebird

// TODO need to figure out a clever way that we can stack things like naming without blowing out
// the number of classes

object Producer {
  def source[P <: StreamPlatform[P], T](s: P#Source[T]): UnkeyedProducer[P, NoState, T] = Source[P, T](s)
}

// State machine for storage etc
sealed trait State
sealed trait NonPlannableState extends State
sealed trait NoState extends NonPlannableState
sealed trait SortedState extends NonPlannableState

sealed trait PlannableState extends State
sealed trait StoreState extends PlannableState

object Wrapper {
  implicit def unkeyedWrapper[P <: StreamPlatform[P], S <: State, T]: Wrapper[P, S, T, UnkeyedProducer[P, S, T]] =
    new Wrapper[P, S, T, UnkeyedProducer[P, S, T]] {
      override def apply(
        p: Producer[P, S, T, UnkeyedProducer[P, S, T]],
        ann: Annotation
      ): UnkeyedProducer[P, S, T] = UnkeyedWrapper(p, ann)
    }

  implicit def keyedWrapper[P <: StreamPlatform[P], S <: State, K, V]: Wrapper[P, S, (K, TraversableOnce[V]), KeyedProducer[P, S, K, V]]  =
    new Wrapper[P, S, (K, TraversableOnce[V]), KeyedProducer[P, S, K, V]] {
      override def apply(
        p: Producer[P, S, (K, TraversableOnce[V]), KeyedProducer[P, S, K, V]],
        ann: Annotation
      ): KeyedProducer[P, S, K, V] = KeyedWrapper(p, ann)
    }
}
sealed trait Wrapper[P <: StreamPlatform[P], S <: State, T, This <: Producer[P, S, T, This]] {
  def apply(p: Producer[P, S, T, This], ann: Annotation): This
}


sealed trait Producer[P <: StreamPlatform[P], S <: State, T, This <: Producer[P, S, T, This]] {
  def name(str: String)(implicit wrap: Wrapper[P, S, T, This]): This = wrap(this, Name(str))
  def write[NewThis <: Producer[P, StoreState, T, NewThis]](store: P#Store[T])
      (implicit wrap: Wrapper[P, StoreState, T, NewThis]): NewThis =
    wrap(this.asInstanceOf[Producer[P, StoreState, T, NewThis]], Store(store))
}

sealed trait UnkeyedProducer[P <: StreamPlatform[P], S <: State, T]
    extends Producer[P, S, T, UnkeyedProducer[P, S, T]] {

  def map[U](fn: T => U): UnkeyedProducer[P, NoState, U] = Map(this, fn)

  def optionMap[U](fn: T => Option[U]): UnkeyedProducer[P, NoState, U] = OptionMap(this, fn)

  def concatMap[U](fn: T => TraversableOnce[U]): UnkeyedProducer[P, NoState, U] = ConcatMap(this, fn)

  def filter(fn: T => Boolean): UnkeyedProducer[P, NoState, T] = Filter(this, fn)

  def group[K, V](implicit ev: T <:< (K, V)): KeyedProducer[P, NoState, K, V] =
    Group(this.asInstanceOf[UnkeyedProducer[P, S, (K, V)]])

  def groupBy[K](fn: T => K): KeyedProducer[P, NoState, K, T] = GroupBy(this, fn)

  def groupAll: KeyedProducer[P, NoState, Unit, T] = GroupAll(this)
}

sealed trait KeyedProducer[P <: StreamPlatform[P], S <: State, K, V]
    extends Producer[P, S, (K, TraversableOnce[V]), KeyedProducer[P, S, K, V]] {

  //def mapGroup[U](fn: (K, TraversibleOnce[V]) => U): UnkeyedProducer[P, NoState, (K, U)] =

  def reduceByKey(fn: (V, V) => V): UnkeyedProducer[P, NoState, (K, V)] = Reducer(this, fn)

  def sort[NewP <: FreePlatform[NewP]]
      (implicit ord: Ordering[V], ev: P <:< NewP): KeyedProducer[NewP, SortedState, K, V] =
    Sorted(this.asInstanceOf[KeyedProducer[NewP, S, K, V]], ord)

  def fold[NewP <: FreePlatform[NewP], U](init: U)(fn: (U, V) => U)
      (implicit ev1: P <:< NewP, ev2: S <:< SortedState): UnkeyedProducer[NewP, NoState, (K, U)] =
    Fold(this.asInstanceOf[KeyedProducer[NewP, SortedState, K, V]], init, fn)
}

case class Source[P <: StreamPlatform[P], T](source: P#Source[T])
  extends UnkeyedProducer[P, NoState, T]

case class Map[P <: StreamPlatform[P], T, U](parent: UnkeyedProducer[P, _ <: State, T], fn: T => U)
  extends UnkeyedProducer[P, NoState, U]

case class OptionMap[P <: StreamPlatform[P], T, U](parent: UnkeyedProducer[P, _ <: State, T], fn: T => Option[U])
  extends UnkeyedProducer[P, NoState, U]

case class ConcatMap[P <: StreamPlatform[P], T, U](parent: UnkeyedProducer[P, _ <: State, T], fn: T => TraversableOnce[U])
  extends UnkeyedProducer[P, NoState, U]

case class Filter[P <: StreamPlatform[P], T](parent: UnkeyedProducer[P, _ <: State, T], fn: T => Boolean)
  extends UnkeyedProducer[P, NoState, T]

case class Group[P <: StreamPlatform[P], K, V](parent: UnkeyedProducer[P, _ <: State, (K, V)])
  extends KeyedProducer[P, NoState, K, V]

case class GroupBy[P <: StreamPlatform[P], K, V](parent: UnkeyedProducer[P, _ <: State, V], fn: V => K)
  extends KeyedProducer[P, NoState, K, V]

case class GroupAll[P <: StreamPlatform[P], V](parent: UnkeyedProducer[P, _ <: State, V])
  extends KeyedProducer[P, NoState, Unit, V]

case class Reducer[P <: StreamPlatform[P], K, V](parent: KeyedProducer[P, _ <: State, K, V], fn: (V, V) => V)
  extends UnkeyedProducer[P, NoState, (K, V)]

case class Sorted[P <: FreePlatform[P], K, V](parent: KeyedProducer[P, _ <: State, K, V], ord: Ordering[V])
  extends KeyedProducer[P, SortedState, K, V]

case class Fold[P <: FreePlatform[P], K, V, U](parent: KeyedProducer[P, SortedState, K, V], init: U, fn: (U, V) => U)
  extends UnkeyedProducer[P, NoState, (K, U)]

sealed trait Annotation
case class Name(str: String) extends Annotation
case class Store[P <: StreamPlatform[P], T](store: P#Store[T]) extends Annotation

case class UnkeyedWrapper[P <: StreamPlatform[P], S <: State, T]
  (wrapped: Producer[P, S, T, UnkeyedProducer[P, S, T]], annotation: Annotation) extends UnkeyedProducer[P, S, T]

case class KeyedWrapper[P <: StreamPlatform[P], S <: State, K, V]
  (wrapped: Producer[P, S, (K, TraversableOnce[V]), KeyedProducer[P, S, K, V]], annotation: Annotation) extends KeyedProducer[P, S, K, V]
