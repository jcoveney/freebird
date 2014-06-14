package com.twitter.freebird

// TODO need to figure out a clever way that we can stack things like naming without blowing out
// the number of classes

object Producer {
  def source[P <: StreamPlatform[P], T](s: P#Source[T]): Producer[P, NoState, Id[T]] = Source[P, T](s)
}

// State machine for storage etc
sealed trait State
sealed trait NonPlannableState extends State
sealed trait NoState extends NonPlannableState
sealed trait SortedState extends NonPlannableState

sealed trait PlannableState extends State
sealed trait StoreState extends PlannableState

// State machine for type states, necessary to separate logical details from physical

// This is something operable
sealed trait Producer[P <: StreamPlatform[P], S <: State, T] {
  def name(str: String): Producer[P, S, R] = Name(this, str)

  def map[T, U](fn: T => U)(implicit ev: R <:< Id[T]): Producer[P, NoState, Id[U]] =
    Map(this.asInstanceOf[Producer[P, S, Id[T]]], fn)

  def optionMap[T, U](fn: T => Option[U])(implicit ev: R <:< Id[T]): Producer[P, NoState, Id[U]] =
    OptionMap(this.asInstanceOf[Producer[P, S, Id[T]]], fn)

  def concatMap[T, U](fn: T => TraversableOnce[U])(implicit ev: R <:< Id[T]): Producer[P, NoState, Id[U]] =
    ConcatMap(this.asInstanceOf[Producer[P, S, Id[T]]], fn)

  def filter[T](fn: T => Boolean)(implicit ev: R <:< Id[T]): Producer[P, NoState, Id[T]] =
    Filter(this.asInstanceOf[Producer[P, S, Id[T]]], fn)

  def group[K, V](implicit ev: R <:< Id[(K, V)]): Producer[P, NoState, Keyed[K, V]] =
    Group(this.asInstanceOf[Producer[P, S, Id[(K, V)]]])

  def groupBy[K, V](fn: V => K)(implicit ev: R <:< Id[V]): Producer[P, NoState, Keyed[K, V]] =
    GroupBy(this.asInstanceOf[Producer[P, S, Id[V]]], fn)

  // ****
  // ****
  //TODO the explosion of Id/Keyed makes it seem pretty clear that it is useful to have
  // 2 traits like we originally did. Need to think about how to get between them.
  // ****
  // ****
  def groupAll[T](implicit ev: R <:< Id[T]): Producer[P, NoState, Keyed[Unit, T]] =
    GroupAll(this.asInstanceOf[Producer[P, S, Id[T]]])

  //TODO is this necessary? Doesn't seem like it...
  def write[T](store: P#Store[T])(implicit ev: R <:< Id[T]): Producer[P, StoreState, Id[T]] =
    Store(this.asInstanceOf[Producer[P, S, Id[T]]], store)

  // TODO what does it mean to write right after a reduceByKey?
  def reduceByKey[K, V](fn: (V, V) => V)(implicit ev: R <:< Keyed[K, V]): Producer[P, NoState, Keyed[K, V]] =
    Reducer(this.asInstanceOf[Producer[P, S, Keyed[K, V]]], fn)

  def sort[NewP <: FreePlatform[NewP], K, V](implicit ord: Ordering[V], ev1: P <:< NewP, ev2: R <:< Keyed[K, V]): Producer[NewP, SortedState, Keyed[K, V]] =
    Sorted(this.asInstanceOf[Producer[NewP, SortedState, Keyed[K, V]]], ord)

  def fold[NewP <: FreePlatform[NewP], K, V, U](init: U)(fn: (U, V) => U)
      (implicit ev1: P <:< NewP, ev2: S <:< SortedState, ev3: R <:< Keyed[K, V]): Producer[NewP, NoState, Keyed[K, U]] =
    Fold(this.asInstanceOf[Producer[NewP, SortedState, Keyed[K, V]]], init, fn)
}

//I propose the following...
sealed trait Producer[P <: StreamPlatform[P], S <: State, T, This <: Producer[P, S, T, This]] {
  def name(str: String): This = Name(this, str)
  def write(store: P#Store[T]): This = Store(this, store)
}

sealed trait UnkeyedProducer[P <: StreamPlatform[P], S <: State, T]
  extends Producer[P, S, T, UnkeyedProducer[P, S, T]]
  
sealed trait KeyedProducer[P <: StreamPlatform[P], S <: State, K, V]
  extends Producer[P, S, (K, V), KeyedProducer[P, S, K, V]]

// AHA! This is the link! since it is a platform, we are golden. we will physically manifest (K, V)'s
sealed trait KeyedProducer[P <: StreamPlatform[P], S <: State, K, V] extends Platform[P, S, (K, V)]

case class Source[P <: StreamPlatform[P], T](source: P#Source[T])
  extends Producer[P, NoState, Id[T]]

case class Map[P <: StreamPlatform[P], T, U](parent: Producer[P, _ <: State, Id[T]], fn: T => U)
  extends Producer[P, NoState, Id[U]]

case class OptionMap[P <: StreamPlatform[P], T, U](parent: Producer[P, _ <: State, Id[T]], fn: T => Option[U])
  extends Producer[P, NoState, Id[U]]

case class ConcatMap[P <: StreamPlatform[P], T, U](parent: Producer[P, _ <: State, Id[T]], fn: T => TraversableOnce[U])
  extends Producer[P, NoState, Id[U]]

case class Filter[P <: StreamPlatform[P], T](parent: Producer[P, _ <: State, Id[T]], fn: T => Boolean)
  extends Producer[P, NoState, Id[T]]

case class Group[P <: StreamPlatform[P], K, V](parent: Producer[P, _ <: State, Id[(K, V)]])
  extends Producer[P, NoState, Keyed[K, V]]

case class GroupBy[P <: StreamPlatform[P], K, V](parent: Producer[P, _ <: State, Id[V]], fn: V => K)
  extends Producer[P, NoState, Keyed[K, V]]

case class GroupAll[P <: StreamPlatform[P], V](parent: Producer[P, _ <: State, Id[V]])
  extends Producer[P, NoState, Keyed[Unit, V]]

case class Reducer[P <: StreamPlatform[P], K, V](parent: Producer[P, _ <: State, Keyed[K, V]], fn: (V, V) => V)
  extends Producer[P, NoState, Keyed[K, V]]

case class Sorted[P <: FreePlatform[P], K, V](parent: Producer[P, _ <: State, Keyed[K, V]], ord: Ordering[V])
  extends Producer[P, SortedState, Keyed[K, V]]

case class Fold[P <: FreePlatform[P], K, V, U](parent: Producer[P, SortedState, Keyed[K, V]], init: U, fn: (U, V) => U)
  extends Producer[P, NoState, Keyed[K, U]]

case class Store[P <: StreamPlatform[P], T](parent: Producer[P, _ <: State, Id[T]], store: P#Store[T])
  extends Producer[P, StoreState, Id[T]]

case class Name[P <: StreamPlatform[P], S <: State, R <: Ret](parent: Producer[P, S, R], name: String)
  extends Producer[P, S, R]
