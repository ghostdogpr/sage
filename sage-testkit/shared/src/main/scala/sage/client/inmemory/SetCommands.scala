package sage.client.inmemory

import scala.util.Random

import sage.protocol.Frame

private[inmemory] object SetCommands {
  import Replies.*
  import Value.Members

  val specs: Map[String, Spec] = Map(
    "SADD"        -> Spec(-3, sAdd),
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
    "SREM"        -> Spec(-3, sRem),
    "SSCAN"       -> Spec(-3, sScan),
    "SUNION"      -> Spec(-2, call => set(combine(call, call.args.indices.map(call.key), _ union _))),
    "SUNIONSTORE" -> Spec(-3, call => storeResult(call, combine(call, call.args.indices.drop(1).map(call.key), _ union _)))
  )

  def members(call: Call, key: Key): Set[Key] = call.typed[Members](key).fold(Set.empty[Key])(_.members)

  private def set(items: Set[Key]): Frame = Frame.Set(items.iterator.map(k => bulk(k.bytes)).toVector)

  // every key is type-checked, including the ones after an empty intermediate result, as on a real server
  private def combine(call: Call, keys: Seq[Key], op: (Set[Key], Set[Key]) => Set[Key]): Set[Key] = {
    val sets = keys.map(members(call, _))
    sets.tail.foldLeft(sets.head)(op)
  }

  private def storeResult(call: Call, result: Set[Key]): Frame = {
    val destination = call.key(0)
    call.store.delete(destination, call.now): Unit
    call.replace(destination, Members(result))
    int(result.size.toLong)
  }

  private def sAdd(call: Call): Frame = {
    val key     = call.key(0)
    val current = members(call, key)
    val added   = call.args.indices.drop(1).map(call.key).toSet -- current
    if (added.nonEmpty) call.replace(key, Members(current ++ added))
    int(added.size.toLong)
  }

  private def sRem(call: Call): Frame = {
    val key     = call.key(0)
    val current = members(call, key)
    val removed = call.args.indices.drop(1).map(call.key).toSet intersect current
    if (removed.nonEmpty) call.replace(key, Members(current -- removed))
    int(removed.size.toLong)
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
        call.replace(source, Members(from - member))
        call.replace(destination, Members(to + member))
      }
      flag(true)
    }
  }

  private def sPop(call: Call): Frame = {
    val key     = call.key(0)
    val current = members(call, key).toVector
    call.args.length match {
      case 1 =>
        if (current.isEmpty) Frame.Null
        else {
          val member = current(Random.nextInt(current.length))
          call.replace(key, Members(current.toSet - member))
          bulk(member.bytes)
        }
      case 2 =>
        val count = call.long(1)
        if (count < 0) throw new Reject(Errors.outOfRange)
        val taken = Sampling.pick(current, count)
        if (taken.nonEmpty) call.replace(key, Members(current.toSet -- taken))
        set(taken.toSet)
      case _ => throw new Reject(Errors.syntax)
    }
  }

  private def sRandMember(call: Call): Frame = {
    val current = members(call, call.key(0)).toVector
    call.args.length match {
      case 1 => if (current.isEmpty) Frame.Null else bulk(current(Random.nextInt(current.length)).bytes)
      case 2 => keys(Sampling.pick(current, call.long(1)))
      case _ => throw new Reject(Errors.syntax)
    }
  }

  private def sInterCard(call: Call): Frame = {
    val (keys, next) = call.numKeys(0)
    val limit        = call.args.length - next match {
      case 0                                  => 0L
      case 2 if call.keyword(next) == "LIMIT" =>
        val n = call.long(next + 1)
        if (n < 0) throw new Reject(Errors.err("LIMIT can't be negative"))
        n
      case _                                  => throw new Reject(Errors.syntax)
    }
    val size         = combine(call, keys, _ intersect _).size.toLong
    int(if (limit == 0) size else math.min(size, limit))
  }

  // small sets are scanned in one page on a real server, so the next cursor is always 0
  private def sScan(call: Call): Frame = {
    Scanning.cursor(call, 1)
    val (pattern, _) = Scanning.options(call, 2)
    val items        = members(call, call.key(0)).toVector.sorted.filter(m => pattern.forall(Glob.matches(_, m.bytes)))
    Frame.Array(Vector(bulk("0"), keys(items)))
  }
}
