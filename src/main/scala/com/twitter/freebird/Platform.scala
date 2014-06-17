package com.twitter.freebird

/**
  * Base trait for freebird compilers.
  */
trait FreePlatform[P <: FreePlatform[P]] extends StreamPlatform[P]

trait StreamPlatform[P <: StreamPlatform[P]] {
  type Source[_]
  type Store[_]
  type Plan[_]
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

  def plan[T, This <: Producer[P, StoreState, T, This]](p: Producer[P, StoreState, T, This]): Plan[T]

  def run[T](plan: Plan[T]): Unit
}
