package sage.client.inmemory

import sage.Bytes
import sage.protocol.Frame

/**
  * `SORT key [BY pattern] [LIMIT offset count] [GET pattern …] [ASC | DESC] [ALPHA] [STORE destination]` and `SORT_RO`, which has no
  * `STORE`. A pattern replaces its first `*` with the element and reads that string key, or reads a hash field when it ends in
  * `->field`. The pattern `#` stands for the element itself. A `BY` pattern without `*` turns sorting off.
  */
private[inmemory] object Sort {
  import Replies.*

  def run(call: Call): Frame = {
    var by       = Option.empty[Bytes]
    var limit    = Option.empty[(Long, Long)]
    var gets     = Vector.empty[Bytes]
    var desc     = false
    var alpha    = false
    var store    = Option.empty[Key]
    var i        = 1
    while (i < call.args.length) {
      val hasValue = i + 1 < call.args.length
      call.keyword(i) match {
        case "BY" if hasValue                           => i += 1; by = Some(call.args(i))
        case "GET" if hasValue                          => i += 1; gets :+= call.args(i)
        case "ASC"                                      => desc = false
        case "DESC"                                     => desc = true
        case "ALPHA"                                    => alpha = true
        case "STORE" if hasValue && call.name == "SORT" => i += 1; store = Some(call.key(i))
        case "LIMIT" if i + 2 < call.args.length        =>
          limit = Some((call.long(i + 1), call.long(i + 2)))
          i += 2
        case _                                          => throw new Reject(Errors.syntax)
      }
      i += 1
    }
    val elements = call.lookup(call.key(0)).map(_.value) match {
      case None                         => Vector.empty[Bytes]
      case Some(Value.Lst(items))       => items
      case Some(Value.Members(members)) => members.toVector.sorted.map(_.bytes)
      case Some(set: Value.Sorted)      => set.ordered.toVector.map(_._2.bytes)
      case Some(_)                      => throw new Reject(Errors.wrongType)
    }
    val sorting  = by.forall(_.toIArray.contains('*'.toByte))
    val sorted   =
      if (!sorting) elements
      else {
        val weights = elements.map(element => by.fold(Option(element))(pattern => lookup(call, pattern, element)))
        val ordered =
          if (alpha) elements.zip(weights).sortWith((x, y) => compareText(x._2, y._2, x._1, y._1) < 0)
          else {
            val scores = weights.map(
              _.fold(0.0)(w => Numbers.parseDouble(w).getOrElse(throw new Reject(Errors.err("One or more scores can't be converted into double"))))
            )
            elements.zip(scores).sortWith((x, y) => compareScore(x._2, y._2, x._1, y._1) < 0)
          }
        val result  = ordered.map(_._1)
        if (desc) result.reverse else result
      }
    val window   = limit.fold(sorted) { (offset, count) =>
      val from = math.max(0L, offset).min(sorted.length.toLong).toInt
      if (count < 0) sorted.drop(from) else sorted.slice(from, from + math.min(count, Int.MaxValue.toLong).toInt)
    }
    val output   = if (gets.isEmpty) window.map(Some(_)) else window.flatMap(e => gets.map(pattern => lookup(call, pattern, e)))
    store match {
      case None              => Frame.Array(output.map(optional))
      case Some(destination) =>
        call.store.delete(destination, call.now): Unit
        // missing GET results are stored as empty strings, as on a real server
        call.replace(destination, Value.Lst(output.map(_.getOrElse(Bytes.empty))))
        int(output.length.toLong)
    }
  }

  private def compareScore(a: Double, b: Double, ea: Bytes, eb: Bytes): Int = {
    val byScore = java.lang.Double.compare(a, b)
    if (byScore != 0) byScore else summon[Ordering[Key]].compare(new Key(ea), new Key(eb))
  }

  // a missing weight sorts before every present one
  private def compareText(a: Option[Bytes], b: Option[Bytes], ea: Bytes, eb: Bytes): Int = {
    val ordering = summon[Ordering[Key]]
    val byWeight = (a, b) match {
      case (None, None)       => 0
      case (None, _)          => -1
      case (_, None)          => 1
      case (Some(x), Some(y)) => ordering.compare(new Key(x), new Key(y))
    }
    if (byWeight != 0) byWeight else ordering.compare(new Key(ea), new Key(eb))
  }

  private def lookup(call: Call, pattern: Bytes, element: Bytes): Option[Bytes] = {
    val text = pattern.toIArray
    if (text.length == 1 && text(0) == '#') Some(element)
    else {
      val star = text.indexOf('*'.toByte)
      if (star < 0) None
      else {
        val arrow            = text.indexOfSlice(IArray[Byte]('-', '>'), star + 1)
        val (keyPart, field) =
          if (arrow < 0 || arrow + 2 >= text.length) (text, None) else (text.take(arrow), Some(text.drop(arrow + 2)))
        val name             = Bytes.wrap(IArray.from(keyPart.take(star) ++ element.toIArray ++ keyPart.drop(star + 1)))
        val key              = new Key(name)
        call.lookup(key).map(_.value).flatMap {
          case Value.Str(bytes)                      => Option.when(field.isEmpty)(bytes)
          case Value.Hash(fields) if field.isDefined => fields.get(new Key(Bytes.wrap(field.get))).map(_.value)
          case _                                     => None
        }
      }
    }
  }
}
