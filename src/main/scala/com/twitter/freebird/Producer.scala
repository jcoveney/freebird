package com.twitter.freebird

// TODO need to figure out a clever way that we can stack things like naming without blowing out
// the number of classes

object Producer {
  def source[P <: StreamPlatform[P], T](s: P#Source[T]): Producer[P, T] = Source[P, T](s)
}

// This is something operable
sealed trait Producer[P <: StreamPlatform[P], T] {
  def map[U](fn: T => U): Producer[P, U] = Map(this, fn)

  def flatMap[U](fn: T => TraversableOnce[U]): Producer[P, U] = FlatMap(this, fn)

  def filter(fn: T => Boolean): Producer[P, T] = Filter(this, fn)

  def group[K, V](implicit ev: T <:< (K, V)): KeyedProducer[P, K, V] =
    Group(this.asInstanceOf[Producer[P, (K, V)]])

  def groupBy[K](fn: T => K): KeyedProducer[P, K, T] =
    map { t => (fn(t), t) }.group

  def groupAll: KeyedProducer[P, Unit, T] = groupBy(_ => Unit)

  def write(store: P#Store[T]): Producer[P, T] = Store(this, store)
}

sealed trait KeyedProducer[P <: StreamPlatform[P], K, V] extends Producer[P, (K, V)] {
  //def sum(semi: Semigroup[V]): KeyedProducer[P, K, V] = Summer(this, semi)

  def reduce(fn: (V, V) => V): KeyedProducer[P, K, V] = Summer(this, fn)

  def sort[T <: FreePlatform[T]](implicit ord: Ordering[V], ev: P <:< T): SortedKeyedProducer[T, K, V] =
    Sorted(this.asInstanceOf[KeyedProducer[T, K, V]], ord)
}

//TODO can we make this another trait that isn't a KeyedProducer?
//TODO can we use the trick scalding did to make it so that they can only sort once?
sealed trait SortedKeyedProducer[P <: FreePlatform[P], K, V] extends KeyedProducer[P, K, V] {
  def fold[U](init: U)(fn: (U, V) => U): Producer[P, (K, U)] = Fold(this, init, fn)
}

case class Source[P <: StreamPlatform[P], T](source: P#Source[T])
  extends Producer[P, T]

case class Map[P <: StreamPlatform[P], T, U](parent: Producer[P, T], fn: T => U)
  extends Producer[P, U]

case class FlatMap[P <: StreamPlatform[P], T, U](parent: Producer[P, T], fn: T => TraversableOnce[U])
  extends Producer[P, U]

case class Filter[P <: StreamPlatform[P], T](parent: Producer[P, T], fn: T => Boolean)
  extends Producer[P, T]

case class Group[P <: StreamPlatform[P], K, V](parent: Producer[P, (K, V)])
  extends KeyedProducer[P, K, V]

case class Summer[P <: StreamPlatform[P], K, V](parent: KeyedProducer[P, K, V], fn: (V, V) => V)
  extends KeyedProducer[P, K, V]

case class Sorted[P <: FreePlatform[P], K, V](parent: KeyedProducer[P, K, V], ord: Ordering[V])
  extends SortedKeyedProducer[P, K, V]

//TODO I don't like the need for a separate Fold and SortedFold
case class Fold[P <: FreePlatform[P], K, V, U](parent: SortedKeyedProducer[P, K, V], init: U, fn: (U, V) => U)
  extends Producer[P, (K, U)]

case class Store[P <: StreamPlatform[P], T](parent: Producer[P, T], store: P#Store[T])
  extends Producer[P, T]
