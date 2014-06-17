package com.twitter.freebird

object Producer {
  def source[P <: StreamPlatform[P], T](s: P#Source[T]): UnkeyedProducer[P, NoState, T] = Source[P, T](s)
}

// State machine for storage etc. If we want more, figure out a way that we can encode
// multiple types of states in the same type without an explosion. I'm thinking State[T, T2, T3] etc.
// This way various states won't trample over each other. This should be reserved for information
// that is incidental, not structural. For example, group status should be encoded in the types,
// but store state can be thrown out. These are overridden by annotations, if they so desire (see Store).
sealed trait State
sealed trait NoState extends State
sealed trait StoreState extends State

sealed trait Producer[P <: StreamPlatform[P], S <: State, T]

// Note that it would be easy to implement a lot of operators in terms of each other, but I want
// the graph to be as rich as possible. I leave it to the planner to do any rewriting for convenience
// (which the memory platform does do)
sealed trait UnkeyedProducer[P <: StreamPlatform[P], S <: State, T]
    extends Producer[P, S, T] {

  // We can probably have a generic function which uses the fact that we know This in order to
  // generically annotate. We will do this if the method explosion gets unwieldy.
  def name(str: String): UnkeyedProducer[P, S, T] = UnkeyedWrapper(this, Name(str))
  def write(store: P#Store[T]): UnkeyedProducer[P, StoreState, T] = UnkeyedWrapper(this, Store(store))

  def map[U](fn: T => U): UnkeyedProducer[P, NoState, U] = Map(this, fn)

  def optionMap[U](fn: T => Option[U]): UnkeyedProducer[P, NoState, U] = OptionMap(this, fn)

  def concatMap[U](fn: T => TraversableOnce[U]): UnkeyedProducer[P, NoState, U] = ConcatMap(this, fn)

  def filter(fn: T => Boolean): UnkeyedProducer[P, NoState, T] = Filter(this, fn)

  def collect[U](fn: PartialFunction[T, U]): UnkeyedProducer[P, NoState, U] = Collect(this, fn)

  def group[K, V](implicit ev: T <:< (K, V)): GroupedProducer[P, NoState, K, V] =
    Group(this.asInstanceOf[UnkeyedProducer[P, S, (K, V)]])

  def groupBy[K](fn: T => K): GroupedProducer[P, NoState, K, T] = GroupBy(this, fn)

  def groupAll: GroupedProducer[P, NoState, Unit, T] = GroupAll(this)

  def ++[U](that: UnkeyedProducer[P, _ <: State, U]): UnkeyedProducer[P, NoState, Either[T, U]] = Merge(this, that)
}

sealed trait KeyedProducer[P <: StreamPlatform[P], S <: State, K, V]
    extends Producer[P, S, P#Keyed[K, V]] {

  def name(str: String): KeyedProducer[P, S, K, V] = KeyedWrapper(this, Name(str))
  def write(store: P#Store[P#Keyed[K, V]]): KeyedProducer[P, StoreState, K, V] = KeyedWrapper(this, Store(store))

  def mapGroup[U](fn: P#Keyed[K, V] => U): UnkeyedProducer[P, NoState, (K, U)] = MapGroup(this, fn)

  def mapValues[U](fn: V => U): KeyedProducer[P, NoState, K, U] = MapValues(this, fn)

  def keys: UnkeyedProducer[P, NoState, K] = Keys(this)

  def values: UnkeyedProducer[P, NoState, V] = Values(this)

  def flatten: UnkeyedProducer[P, NoState, (K, V)] = Flatten(this)

  def unkey: UnkeyedProducer[P, NoState, P#Keyed[K, V]] = Unkey(this)

  def join[NewP <: FreePlatform[NewP], S2 <: State, V2](that: KeyedProducer[P, S2, K, V2])
      (implicit ev: P <:< NewP): KeyedProducer[NewP, NoState, K, (V, V2)] =
    Join(this.asInstanceOf[KeyedProducer[NewP, S, K, V]], that.asInstanceOf[KeyedProducer[NewP, S2, K, V2]])

  def cogroup[NewP <: FreePlatform[NewP], S2 <: State, V2](that: KeyedProducer[P, S2, K, V2])
      (implicit ev: P <:< NewP): UnkeyedProducer[NewP, NoState, (K, (TraversableOnce[V], TraversableOnce[V2]))] =
    CoGroup(this.asInstanceOf[KeyedProducer[NewP, S, K, V]], that.asInstanceOf[KeyedProducer[NewP, S2, K, V2]])

  def reduceByKey(fn: (V, V) => V): UnkeyedProducer[P, NoState, (K, V)] = Reducer(this, fn)

  def foldLeft[NewP <: FreePlatform[NewP], U](init: U)(fn: (U, V) => U)
      (implicit ev: P <:< NewP): UnkeyedProducer[NewP, NoState, (K, U)] =
    Fold(this.asInstanceOf[KeyedProducer[NewP, S, K, V]], init, fn)
}

object GroupedProducer {
  // I chose this approach because making GroupedProducer extends KeyedProducer doesn't play
  // nicely with the F bounded polymorphism, and adding a This to Unkeyed/Keyed/Etc creates
  // a very nasty explosion down the chain.
  implicit def grouped2keyed[P <: StreamPlatform[P], S <: State, K, V]
    (p: GroupedProducer[P, S, K, V]): KeyedProducer[P, S, K, V] = p.toKeyed
}

sealed trait GroupedProducer[P <: StreamPlatform[P], S <: State, K, V]
    extends Producer[P, S, P#Keyed[K, V]] {

  def name(str: String): GroupedProducer[P, S, K, V] = GroupedWrapper(this, Name(str))
  def write(store: P#Store[P#Keyed[K, V]]): GroupedProducer[P, StoreState, K, V] = GroupedWrapper(this, Store(store))

  def toKeyed: KeyedProducer[P, S, K, V] = GroupToKeyed(this)

  def sortValues[NewP <: FreePlatform[NewP]]
      (ord: Ordering[V])(implicit ev: P <:< NewP, d: DummyImplicit): KeyedProducer[NewP, NoState, K, V] =
    sortValues(ord, ev)

  def sortValues[NewP <: FreePlatform[NewP]]
      (implicit ord: Ordering[V], ev: P <:< NewP): KeyedProducer[NewP, NoState, K, V] =
    Sorted(this.asInstanceOf[GroupedProducer[NewP, S, K, V]], ord)
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

case class Collect[P <: StreamPlatform[P], T, U](parent: UnkeyedProducer[P, _ <: State, T], fn: PartialFunction[T, U])
  extends UnkeyedProducer[P, NoState, U]

case class Group[P <: StreamPlatform[P], K, V](parent: UnkeyedProducer[P, _ <: State, (K, V)])
  extends GroupedProducer[P, NoState, K, V]

case class GroupBy[P <: StreamPlatform[P], K, V](parent: UnkeyedProducer[P, _ <: State, V], fn: V => K)
  extends GroupedProducer[P, NoState, K, V]

case class GroupAll[P <: StreamPlatform[P], V](parent: UnkeyedProducer[P, _ <: State, V])
  extends GroupedProducer[P, NoState, Unit, V]

case class Reducer[P <: StreamPlatform[P], K, V](parent: KeyedProducer[P, _ <: State, K, V], fn: (V, V) => V)
  extends UnkeyedProducer[P, NoState, (K, V)]

case class Sorted[P <: FreePlatform[P], K, V](parent: GroupedProducer[P, _ <: State, K, V], ord: Ordering[V])
  extends KeyedProducer[P, NoState, K, V]

case class Fold[P <: FreePlatform[P], K, V, U](parent: KeyedProducer[P, _ <: State, K, V], init: U, fn: (U, V) => U)
  extends UnkeyedProducer[P, NoState, (K, U)]

case class Merge[P <: StreamPlatform[P], T, U](
  left: UnkeyedProducer[P, _ <: State, T],
  right: UnkeyedProducer[P, _ <: State, U]
) extends UnkeyedProducer[P, NoState, Either[T, U]]

case class Join[P <: FreePlatform[P], K, V, V2](
  left: KeyedProducer[P, _ <: State, K, V],
  right: KeyedProducer[P, _ <: State, K, V2]
) extends KeyedProducer[P, NoState, K, (V, V2)]

case class CoGroup[P <: FreePlatform[P], K, V, V2](
  left: KeyedProducer[P, _ <: State, K, V],
  right: KeyedProducer[P, _ <: State, K, V2]
) extends UnkeyedProducer[P, NoState, (K, (TraversableOnce[V], TraversableOnce[V2]))]

case class Flatten[P <: StreamPlatform[P], K, V](parent: KeyedProducer[P, _ <: State, K, V])
  extends UnkeyedProducer[P, NoState, (K, V)]

case class Keys[P <: StreamPlatform[P], K](parent: KeyedProducer[P, _ <: State, K, _])
  extends UnkeyedProducer[P, NoState, K]

case class Values[P <: StreamPlatform[P], V](parent: KeyedProducer[P, _ <: State, _, V])
  extends UnkeyedProducer[P, NoState, V]

case class Unkey[P <: StreamPlatform[P], K, V](parent: KeyedProducer[P, _ <: State, K, V])
  extends UnkeyedProducer[P, NoState, P#Keyed[K, V]]

case class MapValues[P <: StreamPlatform[P], K, V, U](parent: KeyedProducer[P, _ <: State, K, V], fn: V => U)
  extends KeyedProducer[P, NoState, K, U]

case class MapGroup[P <: StreamPlatform[P], K, V, U](
  parent: KeyedProducer[P, _ <: State, K, V],
  fn: P#Keyed[K, V] => U
) extends UnkeyedProducer[P, NoState, (K, U)]

case class GroupToKeyed[P <: StreamPlatform[P], S <: State, K, V](parent: GroupedProducer[P, S, K, V])
  extends KeyedProducer[P, S, K, V]

// Annotations give us the power to embed information in the graph, but also to add constraints, such as
// the StoreState. The annotation type takes precedence in the wrapping however as in the case of Name,
// the type checker will just fill in the types.
sealed trait Annotation[P <: StreamPlatform[P], +S <: State, T]
case class Name[P <: StreamPlatform[P], S <: State, T](str: String) extends Annotation[P, S, T]
case class Store[P <: StreamPlatform[P], T](store: P#Store[T]) extends Annotation[P, StoreState, T]

case class UnkeyedWrapper[P <: StreamPlatform[P], S1 <: State, S2 <: State, T]
    (wrapped: UnkeyedProducer[P, S1, T], annotation: Annotation[P, S2, T])
  extends UnkeyedProducer[P, S2, T]

case class KeyedWrapper[P <: StreamPlatform[P], S1 <: State, S2 <: State, K, V](
  wrapped: KeyedProducer[P, S1, K, V],
  annotation: Annotation[P, S2, P#Keyed[K, V]]
) extends KeyedProducer[P, S2, K, V]

case class GroupedWrapper[P <: StreamPlatform[P], S1 <: State, S2 <: State, K, V](
  wrapped: GroupedProducer[P, S1, K, V],
  annotation: Annotation[P, S2, P#Keyed[K, V]]
) extends GroupedProducer[P, S2, K, V]
