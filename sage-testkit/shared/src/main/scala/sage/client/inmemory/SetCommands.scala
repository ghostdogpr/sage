package sage.client.inmemory

import sage.protocol.Frame

private[inmemory] object SetCommands {
  import Replies.*
  import Value.Members

  val specs: Map[String, Spec] = Map(
    "SADD"       -> Spec(-3, changeMembers(_, add = true)),
    "SCARD"      -> Spec(2, call => int(members(call, call.key(0)).size.toLong)),
    "SDIFF"      -> Spec(-2, combine(_, _ -- _)),
    "SINTER"     -> Spec(-2, combine(_, _ intersect _)),
    "SISMEMBER"  -> Spec(3, call => flag(members(call, call.key(0)).contains(call.key(1)))),
    "SMEMBERS"   -> Spec(2, call => set(members(call, call.key(0)))),
    "SMISMEMBER" -> Spec(-3, sMisMember),
    "SREM"       -> Spec(-3, changeMembers(_, add = false)),
    "SSCAN"      -> Spec(-3, sScan),
    "SUNION"     -> Spec(-2, combine(_, _ union _))
  )

  private def members(call: Call, key: Key): Set[Key] = call.typed[Members](key).fold(Set.empty[Key])(_.members)

  private def set(items: Set[Key]): Frame = Frame.Set(items.iterator.map(k => bulk(k.bytes)).toVector)

  // every key is type-checked, including the ones after an empty intermediate result, as on a real server
  private def combine(call: Call, op: (Set[Key], Set[Key]) => Set[Key]): Frame =
    set(call.args.indices.map(i => members(call, call.key(i))).reduceLeftOption(op).getOrElse(Set.empty))

  private def changeMembers(call: Call, add: Boolean): Frame = {
    val key     = call.key(0)
    val current = members(call, key)
    val named   = call.args.indices.drop(1).map(call.key)
    val updated = if (add) current ++ named else current -- named
    val changed = (updated.size - current.size).abs
    if (changed > 0) call.put(key, Members(updated))
    int(changed.toLong)
  }

  private def sMisMember(call: Call): Frame = {
    val current = members(call, call.key(0))
    Frame.Array(call.args.indices.drop(1).toVector.map(i => flag(current.contains(call.key(i)))))
  }

  private def sScan(call: Call): Frame =
    Scanning.page(call, call.typed[Members](call.key(0))) { (set, options) =>
      set.members.toVector.sorted.filter(m => options.matches(m.bytes)).map(m => bulk(m.bytes))
    }
}
