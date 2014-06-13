package com.twitter.freebird

// TODO need to figure out a clever way that we can stack things like naming without blowing out
// the number of classes

object Producer {
  def source[P <: StreamPlatform[P], T](s: P#Source[T]): Producer[P, NoState, T] = Source[P, T](s)
}

sealed trait State
sealed trait NonPlannableState extends State
sealed trait NoState extends NonPlannableState
sealed trait SortedState extends NonPlannableState

sealed trait PlannableState extends State
sealed trait StoreState extends PlannableState

// This is something operable
sealed trait Producer[P <: StreamPlatform[P], S <: State, T] {
  def map[U](fn: T => U): Producer[P, NoState, U] = Map(this, fn)

  def flatMap[U](fn: T => TraversableOnce[U]): Producer[P, NoState, U] = FlatMap(this, fn)

  def filter(fn: T => Boolean): Producer[P, NoState, T] = Filter(this, fn)

  def group[K, V](implicit ev: T <:< (K, V)): KeyedProducer[P, NoState, K, V] =
    Group(this.asInstanceOf[Producer[P, S, (K, V)]])

  def groupBy[K](fn: T => K): KeyedProducer[P, NoState, K, T] = map { t => (fn(t), t) }.group

  def groupAll: KeyedProducer[P, NoState, Unit, T] = groupBy(_ => Unit)

  // Should return itself, right? Might be too hard to do...
  def write(store: P#Store[T]): Producer[P, StoreState, T] = Store(this, store)
}

sealed trait KeyedProducer[P <: StreamPlatform[P], S <: State, K, V] extends Producer[P, S, (K, V)] {
  //def sum(semi: Semigroup[V]): KeyedProducer[P, K, V] = Summer(this, semi)

  def reduce(fn: (V, V) => V): KeyedProducer[P, NoState, K, V] = Summer(this, fn)

  def sort[T <: FreePlatform[T]](implicit ord: Ordering[V], ev: P <:< T): KeyedProducer[T, SortedState, K, V] =
    Sorted(this.asInstanceOf[KeyedProducer[T, SortedState, K, V]], ord)

  def fold[T <: FreePlatform[T], U](init: U)(fn: (U, V) => U)(implicit ev1: P <:< T, ev2: S <:< SortedState): Producer[T, NoState, (K, U)] =
    Fold(this.asInstanceOf[KeyedProducer[T, SortedState, K, V]], init, fn)
}

case class Source[P <: StreamPlatform[P], T](source: P#Source[T])
  extends Producer[P, NoState, T]

case class Map[P <: StreamPlatform[P], T, U](parent: Producer[P, _ <: State, T], fn: T => U)
  extends Producer[P, NoState, U]

case class FlatMap[P <: StreamPlatform[P], T, U](parent: Producer[P, _ <: State, T], fn: T => TraversableOnce[U])
  extends Producer[P, NoState, U]

case class Filter[P <: StreamPlatform[P], T](parent: Producer[P, _ <: State, T], fn: T => Boolean)
  extends Producer[P, NoState, T]

case class Group[P <: StreamPlatform[P], K, V](parent: Producer[P, _ <: State, (K, V)])
  extends KeyedProducer[P, NoState, K, V]

case class Summer[P <: StreamPlatform[P], K, V](parent: KeyedProducer[P, _ <: State, K, V], fn: (V, V) => V)
  extends KeyedProducer[P, NoState, K, V]

case class Sorted[P <: FreePlatform[P], K, V](parent: KeyedProducer[P, _ <: State, K, V], ord: Ordering[V])
  extends KeyedProducer[P, SortedState, K, V]

case class Fold[P <: FreePlatform[P], K, V, U](parent: KeyedProducer[P, SortedState, K, V], init: U, fn: (U, V) => U)
  extends Producer[P, NoState, (K, U)]

case class Store[P <: StreamPlatform[P], T](parent: Producer[P, _ <: State, T], store: P#Store[T])
  extends Producer[P, StoreState, T]
