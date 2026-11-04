package sage.client.inmemory

import sage.protocol.Frame

private[inmemory] object SetCommands {
  import Replies.*
  import Value.Members

  val specs: Map[String, Spec] = Map(
    "SADD"        -> Spec(-3, changeMembers(_, add = true)),
    "SCARD"       -> Spec(2, call => int(members(call, call.key(0)).size.toLong)),
    "SDIFF"       -> Spec(-2, call => set(combine(call, call.args.indices.map(call.key), _ -- _))),
    "SDIFFSTORE"  -> Spec(-3, call => storeResult(call, combine(call, call.args.indices.drop(1).map(call.key), _ -- _))),
    "SINTER"      -> Spec(-2, call => set(combine(call, call.args.indices.map(call.key), _ intersect _))),
    "SINTERCARD"  -> Spec(-3, sInterCard),
    "SINTERSTORE" -> Spec(-3, call => storeResult(call, combine(call, call.args.indices.drop(1).map(call.key), _ intersect _))),
    "SISMEMBER"   -> Spec(3, call => flag(members(call, call.key(0)).contains(call.key(1)))),
    "SMEMBERS"    -> Spec(2, call => set(members(call, call.key(0)))),
    "SMISMEMBER"  -> Spec(-3, sMisMember),
    "SMOVE"       -> Spec(4, sMove),
    "SPOP"        -> Spec(-2, sPop),
    "SRANDMEMBER" -> Spec(-2, sRandMember),
    "SREM"        -> Spec(-3, changeMembers(_, add = false)),
    "SSCAN"       -> Spec(-3, sScan),
    "SUNION"      -> Spec(-2, call => set(combine(call, call.args.indices.map(call.key), _ union _))),
    "SUNIONSTORE" -> Spec(-3, call => storeResult(call, combine(call, call.args.indices.drop(1).map(call.key), _ union _)))
  )

  def members(call: Call, key: Key): Set[Key] = call.typed[Members](key).fold(Set.empty[Key])(_.members)

  private def set(items: Set[Key]): Frame = Frame.Set(items.iterator.map(k => bulk(k.bytes)).toVector)

  // every key is type-checked, including the ones after an empty intermediate result, as on a real server
  private def combine(call: Call, keys: Seq[Key], op: (Set[Key], Set[Key]) => Set[Key]): Set[Key] =
    keys.map(members(call, _)).reduceLeftOption(op).getOrElse(Set.empty)

  private def storeResult(call: Call, result: Set[Key]): Frame = call.stored(call.key(0), Members(result), result.size)

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

  private def sMove(call: Call): Frame = {
    val source      = call.key(0)
    val destination = call.key(1)
    val member      = call.key(2)
    val from        = members(call, source)
    val to          = members(call, destination)
    if (!from.contains(member)) flag(false)
    else {
      if (source != destination) {
        call.put(source, Members(from - member))
        call.put(destination, Members(to + member))
      }
      flag(true)
    }
  }

  private def sPop(call: Call): Frame = {
    val key     = call.key(0)
    val count   = call.optional(1)(call.bounded(_, Errors.outOfRange)(_ >= 0))
    val current = members(call, key).toVector
    val taken   = count.fold(Sampling.one(current).toVector)(Sampling.pick(current, _))
    if (taken.nonEmpty) call.put(key, Members(current.toSet -- taken))
    count.fold(optional(taken.headOption.map(_.bytes)))(_ => set(taken.toSet))
  }

  private def sRandMember(call: Call): Frame = {
    val count   = call.optional(1)(call.negatable)
    val current = members(call, call.key(0)).toVector
    count.fold(optional(Sampling.one(current).map(_.bytes)))(n => keys(Sampling.pick(current, n)))
  }

  private def sInterCard(call: Call): Frame = {
    val keys = call.numKeys(0, Errors.err("Number of keys can't be greater than number of args"))
    interCard(call)(combine(call, keys, _ intersect _).size.toLong)
  }

  // the `[LIMIT limit]` of SINTERCARD and ZINTERCARD after their keys, where a server takes the last of repeated limits
  def interCard(call: Call)(total: => Long): Frame = {
    var limit = 0L
    call.options(call.position) {
      case "LIMIT" => limit = call.nextBounded(Errors.err("LIMIT can't be negative"))(_ >= 0)
      case _       => throw new Reject(Errors.syntax)
    }
    val count = total
    int(if (limit == 0) count else math.min(count, limit))
  }

  private def sScan(call: Call): Frame =
    Scanning.page(call, call.typed[Members](call.key(0))) { (set, options) =>
      set.members.toVector.sorted.filter(m => options.matches(m.bytes)).map(m => bulk(m.bytes))
    }
}
