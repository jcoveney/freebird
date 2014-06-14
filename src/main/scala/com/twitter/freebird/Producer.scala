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

sealed trait Keyed[K, V]

// This is something operable
sealed trait Producer[P <: StreamPlatform[P], S <: State, T] {
  def name(str: String): Producer[P, S, T] = Name(this, str)

  def map[U](fn: T => U): Producer[P, NoState, U] = Map(this, fn)

  def optionMap[U](fn: T => Option[U]): Producer[P, NoState, U] = OptionMap(this, fn)

  def concatMap[U](fn: T => TraversableOnce[U]): Producer[P, NoState, U] = ConcatMap(this, fn)

  def filter(fn: T => Boolean): Producer[P, NoState, T] = Filter(this, fn)

  def group[K, V](implicit ev: T <:< (K, V)): Producer[P, NoState, Keyed[K, V]] =
    Group(this.asInstanceOf[Producer[P, S, (K, V)]])

  def groupBy[K](fn: T => K): Producer[P, NoState, Keyed[K, T]] = GroupBy(this, fn)

  def groupAll: Producer[P, NoState, Keyed[Unit, T]] = GroupAll(this)

  // Should return itself, right? Might be too hard to do...
  def write(store: P#Store[T]): Producer[P, StoreState, T] = Store(this, store)

  def reduceByKey[K, V](fn: (V, V) => V)(implicit ev: T <:< Keyed[K, V]): Producer[P, NoState, Keyed[K, V]] =
    Reducer(this.asInstanceOf[Producer[P, S, Keyed[K, V]]], fn)

  def sort[NewP <: FreePlatform[NewP], K, V](implicit ord: Ordering[V], ev1: P <:< NewP, ev2: T <:< Keyed[K, V]): Producer[NewP, SortedState, Keyed[K, V]] =
    Sorted(this.asInstanceOf[Producer[NewP, SortedState, Keyed[K, V]]], ord)

  def fold[NewP <: FreePlatform[NewP], K, V, U](init: U)(fn: (U, V) => U)
      (implicit ev1: P <:< NewP, ev2: S <:< SortedState, ev3: T <:< Keyed[K, V]): Producer[NewP, NoState, Keyed[K, U]] =
    Fold(this.asInstanceOf[Producer[NewP, SortedState, Keyed[K, V]]], init, fn)
}

case class Source[P <: StreamPlatform[P], T](source: P#Source[T])
  extends Producer[P, NoState, T]

case class Map[P <: StreamPlatform[P], T, U](parent: Producer[P, _ <: State, T], fn: T => U)
  extends Producer[P, NoState, U]

case class OptionMap[P <: StreamPlatform[P], T, U](parent: Producer[P, _ <: State, T], fn: T => Option[U])
  extends Producer[P, NoState, U]

case class ConcatMap[P <: StreamPlatform[P], T, U](parent: Producer[P, _ <: State, T], fn: T => TraversableOnce[U])
  extends Producer[P, NoState, U]

case class Filter[P <: StreamPlatform[P], T](parent: Producer[P, _ <: State, T], fn: T => Boolean)
  extends Producer[P, NoState, T]

case class Group[P <: StreamPlatform[P], K, V](parent: Producer[P, _ <: State, (K, V)])
  extends Producer[P, NoState, Keyed[K, V]]

case class GroupBy[P <: StreamPlatform[P], K, V](parent: Producer[P, _ <: State, V], fn: V => K)
  extends Producer[P, NoState, Keyed[K, V]]

case class GroupAll[P <: StreamPlatform[P], V](parent: Producer[P, _ <: State, V])
  extends Producer[P, NoState, Keyed[Unit, V]]

case class Reducer[P <: StreamPlatform[P], K, V](parent: Producer[P, _ <: State, Keyed[K, V]], fn: (V, V) => V)
  extends Producer[P, NoState, Keyed[K, V]]

case class Sorted[P <: FreePlatform[P], K, V](parent: Producer[P, _ <: State, Keyed[K, V]], ord: Ordering[V])
  extends Producer[P, SortedState, Keyed[K, V]]

case class Fold[P <: FreePlatform[P], K, V, U](parent: Producer[P, SortedState, Keyed[K, V]], init: U, fn: (U, V) => U)
  extends Producer[P, NoState, Keyed[K, U]]

case class Store[P <: StreamPlatform[P], T](parent: Producer[P, _ <: State, T], store: P#Store[T])
  extends Producer[P, StoreState, T]

case class Name[P <: StreamPlatform[P], S <: State, T](parent: Producer[P, S, T], name: String)
  extends Producer[P, S, T]
