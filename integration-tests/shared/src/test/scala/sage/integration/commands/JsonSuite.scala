package sage.integration.commands

import kyo.compat.*

import sage.SageException.DecodeError
import sage.codec.ValueCodec
import sage.commands.{JsonPath, JsonSetCondition, JsonType}
import sage.integration.{Images, ServerSuite}
import sage.protocol.Frame

final case class JsonAddress(city: String, zip: String)
final case class JsonPerson(name: String, age: Int, address: JsonAddress)

abstract class JsonSuite(image: String) extends ServerSuite(image) {

  test("JSON.SET and JSON.GET store and read a document, honoring NX/XX") {
    withClient { client =>
      for {
        set    <- client.jsonSet("doc", JsonPath.root, """{"a":1,"s":"hi"}""")
        nxSkip <- client.jsonSet("doc", JsonPath.root, """{"a":2}""", JsonSetCondition.IfNotExists)
        whole  <- client.jsonGet[String]("doc")
        field  <- client.jsonGet[String]("doc", JsonPath("$.a"))
        absent <- client.jsonGet[String]("missing")
      } yield {
        assert(set)
        assert(!nxSkip)
        assert(whole.exists(_.contains("\"a\"")))
        assert(field.exists(_.contains("1")))
        assertEquals(absent, None)
      }
    }
  }

  test("JSON.TYPE, JSON.OBJKEYS, JSON.OBJLEN inspect structure") {
    withClient { client =>
      for {
        _    <- client.jsonSet("shape", JsonPath.root, """{"a":1,"b":true,"c":"x"}""")
        tpe  <- client.jsonType("shape", JsonPath("$.a"))
        keys <- client.jsonObjKeys("shape")
        len  <- client.jsonObjLen("shape")
        none <- client.jsonType("shape", JsonPath("$.missing"))
      } yield {
        assertEquals(tpe, Vector(Some(JsonType.Integer)))
        assert(keys.headOption.flatten.exists(_.toSet == Set("a", "b", "c")))
        assertEquals(len, Vector(Some(3L)))
        assertEquals(none, Vector.empty)
      }
    }
  }

  test("numeric, string, and boolean mutations return per-match results") {
    withClient { client =>
      for {
        _      <- client.jsonSet("scalars", JsonPath.root, """{"n":1,"s":"ab","b":false}""")
        incr   <- client.jsonNumIncrBy("scalars", JsonPath("$.n"), 4.0)
        mult   <- client.jsonNumMultBy("scalars", JsonPath("$.n"), 2.0)
        strLen <- client.jsonStrAppend("scalars", JsonPath("$.s"), "\"cd\"")
        len    <- client.jsonStrLen("scalars", JsonPath("$.s"))
        toggle <- client.jsonToggle("scalars", JsonPath("$.b"))
      } yield {
        assertEquals(incr, Vector(Some(5.0)))
        assertEquals(mult, Vector(Some(10.0)))
        assertEquals(strLen, Vector(Some(4L)))
        assertEquals(len, Vector(Some(4L)))
        assertEquals(toggle, Vector(Some(true)))
      }
    }
  }

  test("a multi-match path returns one entry per match for JSON.TYPE and JSON.NUMINCRBY") {
    withClient { client =>
      for {
        _    <- client.jsonSet("multi", JsonPath.root, """{"a":{"x":1},"b":{"x":"s"}}""")
        tpe  <- client.jsonType("multi", JsonPath("$..x"))
        _    <- client.jsonSet("nums", JsonPath.root, """{"a":{"x":1},"b":{"x":2}}""")
        incr <- client.jsonNumIncrBy("nums", JsonPath("$..x"), 5.0)
      } yield {
        assertEquals(tpe.toSet, Set(Option(JsonType.Integer), Option(JsonType.String)))
        assertEquals(incr.flatten.toSet, Set(6.0, 7.0))
      }
    }
  }

  test("array commands append, index, insert, pop, trim, and length") {
    withClient { client =>
      for {
        _      <- client.jsonSet("arr", JsonPath.root, """{"xs":[1,2,3]}""")
        appLen <- client.jsonArrAppend("arr", JsonPath("$.xs"), "4", "5")
        idx    <- client.jsonArrIndex("arr", JsonPath("$.xs"), "3")
        insLen <- client.jsonArrInsert("arr", JsonPath("$.xs"), 0L, "0")
        len    <- client.jsonArrLen("arr", JsonPath("$.xs"))
        popped <- client.jsonArrPop[String]("arr", JsonPath("$.xs"))
        trim   <- client.jsonArrTrim("arr", JsonPath("$.xs"), 0L, 1L)
      } yield {
        assertEquals(appLen, Vector(Some(5L)))
        assertEquals(idx, Vector(Some(2L)))
        assertEquals(insLen, Vector(Some(6L)))
        assertEquals(len, Vector(Some(6L)))
        assert(popped.headOption.flatten.exists(_.contains("5")))
        assertEquals(trim, Vector(Some(2L)))
      }
    }
  }

  test("JSON.MGET, JSON.MSET, JSON.DEL, JSON.CLEAR, JSON.DEBUG MEMORY, JSON.RESP") {
    withClient { client =>
      for {
        _     <- client.jsonMSet(("m1", JsonPath.root, """{"v":1}"""), ("m2", JsonPath.root, """{"v":2}"""))
        mget  <- client.jsonMGet[String](JsonPath("$.v"))("m1", "m2", "m3")
        del   <- client.jsonDel("m1", JsonPath("$.v"))
        clear <- client.jsonClear("m2", JsonPath.root)
        mem   <- client.jsonDebugMemory("m2")
        resp  <- client.jsonResp("m2")
      } yield {
        assert(mget(0).exists(_.contains("1")))
        assert(mget(1).exists(_.contains("2")))
        assertEquals(mget(2), None)
        assertEquals(del, 1L)
        assertEquals(clear, 1L)
        assert(mem.headOption.flatten.exists(_ > 0L))
        assertNotEquals(resp, Frame.Null: Frame)
      }
    }
  }

  test("a user-supplied JSON codec (circe) round-trips typed documents") {
    import io.circe.generic.auto.*
    import io.circe.parser.decode
    import io.circe.syntax.*
    given [A](using io.circe.Decoder[A], io.circe.Encoder[A]): ValueCodec[A] =
      ValueCodec.string.emap(s => decode[A](s).left.map(DecodeError.fromThrowable))(_.asJson.noSpaces)

    val alice = JsonPerson("Alice", 30, JsonAddress("NYC", "10001"))
    withClient { client =>
      for {
        _     <- client.jsonSet("person:1", JsonPath.root, alice)
        whole <- client.jsonGet[JsonPerson]("person:1")
        ages  <- client.jsonGet[Vector[Int]]("person:1", JsonPath("$.age"))
      } yield {
        assertEquals(whole, Some(alice))
        assertEquals(ages, Some(Vector(30)))
      }
    }
  }
  test("a legacy (non-$) path fails with a clear typed error, not silent data") {
    withClient { client =>
      client.jsonSet("legacy", JsonPath.root, """{"xs":[1,2,3]}""").flatMap(_ => client.jsonArrLen("legacy", JsonPath(".xs")))
    }.failed.map { error =>
      assert(error.isInstanceOf[DecodeError], s"expected a DecodeError, got $error")
      assert(error.getMessage.contains("legacy"), s"error should name the legacy-path cause: ${error.getMessage}")
    }
  }
}

class RedisJsonSuite extends JsonSuite(Images.redis)

class ValkeyJsonSuite extends JsonSuite(Images.valkeyBundle)
