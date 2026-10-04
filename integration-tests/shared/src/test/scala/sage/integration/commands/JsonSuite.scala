package sage.integration.commands

import com.dimafeng.testcontainers.GenericContainer
import kyo.compat.*

import sage.SageException.DecodeError
import sage.codec.ValueCodec
import sage.commands.{JsonPath, JsonSetCondition, JsonType}
import sage.integration.{BothServersSuite, Images}
import sage.protocol.Frame

final case class JsonAddress(city: String, zip: String)
final case class JsonPerson(name: String, age: Int, address: JsonAddress)

class JsonSuite extends BothServersSuite {

  override protected def valkeyDef: GenericContainer.Def[GenericContainer] = serverDef(Images.valkeyBundle)

  clientTest("JSON.SET and JSON.GET store and read a document, honoring NX/XX") { client =>
    client.jsonSet("doc", JsonPath.root, """{"a":1,"s":"hi"}""").is(true) >>
      client.jsonSet("doc", JsonPath.root, """{"a":2}""", JsonSetCondition.IfNotExists).is(false) >>
      client.jsonGet[String]("doc").is(Some("""{"a":1,"s":"hi"}""")) >>
      client.jsonGet[String]("doc", JsonPath("$.a")).is(Some("[1]")) >>
      client.jsonGet[String]("missing").is(None)
  }

  clientTest("JSON.TYPE, JSON.OBJKEYS, JSON.OBJLEN inspect structure") { client =>
    client.jsonSet("shape", JsonPath.root, """{"a":1,"b":true,"c":"x"}""") >>
      client.jsonType("shape", JsonPath("$.a")).is(Vector(Some(JsonType.Integer))) >>
      client.jsonObjKeys("shape").is(Vector(Some(Vector("a", "b", "c")))) >>
      client.jsonObjLen("shape").is(Vector(Some(3L))) >>
      client.jsonType("shape", JsonPath("$.missing")).is(Vector.empty)
  }

  clientTest("numeric, string, and boolean mutations return per-match results") { client =>
    client.jsonSet("scalars", JsonPath.root, """{"n":1,"s":"ab","b":false}""") >>
      client.jsonNumIncrBy("scalars", JsonPath("$.n"), 4.0).is(Vector(Some(5.0))) >>
      client.jsonNumMultBy("scalars", JsonPath("$.n"), 2.0).is(Vector(Some(10.0))) >>
      client.jsonStrAppend("scalars", JsonPath("$.s"), "\"cd\"").is(Vector(Some(4L))) >>
      client.jsonStrLen("scalars", JsonPath("$.s")).is(Vector(Some(4L))) >>
      client.jsonToggle("scalars", JsonPath("$.b")).is(Vector(Some(true)))
  }

  clientTest("a multi-match path returns one entry per match for JSON.TYPE and JSON.NUMINCRBY") { client =>
    client.jsonSet("multi", JsonPath.root, """{"a":{"x":1},"b":{"x":"s"}}""") >>
      client.jsonType("multi", JsonPath("$..x")).map(_.toSet).is(Set(Option(JsonType.Integer), Option(JsonType.String))) >>
      client.jsonSet("nums", JsonPath.root, """{"a":{"x":1},"b":{"x":2}}""") >>
      client.jsonNumIncrBy("nums", JsonPath("$..x"), 5.0).map(_.flatten.toSet).is(Set(6.0, 7.0))
  }

  clientTest("array commands append, index, insert, pop, trim, and length") { client =>
    client.jsonSet("arr", JsonPath.root, """{"xs":[1,2,3]}""") >>
      client.jsonArrAppend("arr", JsonPath("$.xs"), "4", "5").is(Vector(Some(5L))) >>
      client.jsonArrIndex("arr", JsonPath("$.xs"), "3").is(Vector(Some(2L))) >>
      client.jsonArrInsert("arr", JsonPath("$.xs"), 0L, "0").is(Vector(Some(6L))) >>
      client.jsonArrLen("arr", JsonPath("$.xs")).is(Vector(Some(6L))) >>
      client.jsonArrPop[String]("arr", JsonPath("$.xs")).is(Vector(Some("5"))) >>
      client.jsonArrTrim("arr", JsonPath("$.xs"), 0L, 1L).is(Vector(Some(2L)))
  }

  clientTest("JSON.MGET, JSON.MSET, JSON.DEL, JSON.CLEAR, JSON.DEBUG MEMORY, JSON.RESP") { client =>
    for {
      _    <- client.jsonMSet(("m1", JsonPath.root, """{"v":1}"""), ("m2", JsonPath.root, """{"v":2}"""))
      _    <- client.jsonMGet[String](JsonPath("$.v"))("m1", "m2", "m3").is(Vector(Some("[1]"), Some("[2]"), None))
      _    <- client.jsonDel("m1", JsonPath("$.v")).is(1L)
      _    <- client.jsonClear("m2", JsonPath.root).is(1L)
      _    <- client.jsonDebugMemory("m2").satisfies(_.headOption.flatten.exists(_ > 0L))
      resp <- client.jsonResp("m2")
    } yield assertNotEquals(resp, Frame.Null: Frame)
  }

  clientTest("a user-supplied JSON codec (circe) round-trips typed documents") { client =>
    import io.circe.generic.auto.*
    import io.circe.parser.decode
    import io.circe.syntax.*
    given [A](using io.circe.Decoder[A], io.circe.Encoder[A]): ValueCodec[A] =
      ValueCodec.string.emap(s => decode[A](s).left.map(DecodeError.fromThrowable))(_.asJson.noSpaces)

    val alice = JsonPerson("Alice", 30, JsonAddress("NYC", "10001"))
    client.jsonSet("person:1", JsonPath.root, alice) >>
      client.jsonGet[JsonPerson]("person:1").is(Some(alice)) >>
      client.jsonGet[Vector[Int]]("person:1", JsonPath("$.age")).is(Some(Vector(30)))
  }
  clientTest("a legacy (non-$) path fails with a clear typed error, not silent data") { client =>
    failsWith[DecodeError](
      client.jsonSet("legacy", JsonPath.root, """{"xs":[1,2,3]}""").flatMap(_ => client.jsonArrLen("legacy", JsonPath(".xs")))
    ).satisfies(_.getMessage.contains("legacy"))
  }

  // JSON.MERGE exists on Redis but not on Valkey Bundle.
  redisTest("JSON.MERGE updates existing and creates new members") { client =>
    client.jsonSet("merge", JsonPath.root, """{"a":1,"b":2}""") >>
      client.jsonMerge("merge", JsonPath.root, """{"b":20,"c":3}""") >>
      client.jsonGet[String]("merge").is(Some("""{"a":1,"b":20,"c":3}"""))
  }
}
