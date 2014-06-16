package com.twitter.freebird

class MRPlatform extends FreePlatform[MRPlatform] {
  private[this] def inPlan[T, This <: Producer[MRPlatform, _ <: State, T, This]](
    p: Producer[MRPlatform, _ <: State, T, This]
  ): MRPhysical[T] =
    p match {
      case Source(source)         => SourceMP(source)
      case ConcatMap(parent, fn)  => ConcatMapMP(inPlan(parent), fn)
      //case Group(parent)          => GroupMP(inPlan(parent))
      //case CoGroup(left, right)   => CoGroupMP(inPlan(left), inPlan(right))
      //case Merge(left, right)     => MergeMP(inPlan(left), inPlan(right))
      //case OptionMap(parent, fn)  => inPlan(parent.concatMap(fn(_).toList))
      //case Map(parent, fn)        => inPlan(parent.concatMap { v => List(fn(v)) })
      //case Filter(parent, fn)     => inPlan(parent.concatMap { v => if (fn(v)) List(v) else Nil })
      //case Collect(parent, fn)    => inPlan(parent.optionMap(fn.lift))
      //case GroupBy(parent, fn)    => inPlan(parent.map { v => (fn(v), v) }.group)
      //case GroupAll(parent)       => inPlan(parent.groupBy { _ => Unit })
      //case Unkey(parent)          => inPlan(parent)
      //case GroupToKeyed(parent)   => inPlan(parent)
      //case Flatten(parent)        => inPlan(parent.unkey.concatMap { case (k, v) => v.map { (k, _) } })
      //case Keys(parent)           => inPlan(parent.unkey.map(_._1))
      //case Values(parent)         => inPlan(parent.unkey.concatMap(_._2))
      //case MapValues(parent, fn)  => inPlan(parent.unkey.map { case (k, v) => (k, v.map(fn).toList) })
      //case MapGroup(parent, fn)   => inPlan(parent.unkey.map { case (k, v) => (k, fn(k, v)) })
      //case Reducer(parent, fn)    => inPlan(parent.unkey.map { case (k, v) => (k, v.reduce(fn))})
      //case Fold(parent, init, fn) => inPlan(parent.unkey.map { case (k, v) => (k, v.foldLeft(init)(fn))})
      //case Sorted(parent, ord)    => inPlan(parent.unkey.map { case (k, v) => (k, v.toIndexedSeq.sorted(ord))})
      //case UnkeyedWrapper(parent, Name(str))    => inPlan(parent)
      //case KeyedWrapper(parent, Name(str))      => inPlan(parent)
      //case GroupedWrapper(parent, Name(str))    => inPlan(parent)
      //case UnkeyedWrapper(parent, Store(store)) => StoreMP(inPlan(parent), store)
      //case KeyedWrapper(parent, Store(store))   => StoreMP(inPlan(parent), store)
      //case GroupedWrapper(parent, Store(store)) => StoreMP(inPlan(parent), store)
      //case Join(left, right) =>
      //  inPlan(left.cogroup(right).concatMap { case (k, (lft, rght)) =>
      //    lft.flatMap { l => rght.map { r => (k, (l, r)) } }
      //  }.group)
    }

  override def plan[T, This <: Producer[MRPlatform, StoreState, T, This]]
    (p: Producer[MRPlatform, StoreState, T, This]) = inPlan(p)

  override def run[T](plan: MRPhysical[T]) {
    //TODO implement
  }
}