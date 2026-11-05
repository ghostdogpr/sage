package sage.commands

class JsonSpec extends munit.FunSuite with BothProtocols {

  test("JSON.TYPE decodes the flat RESP2 and Valkey replies like the nested Redis RESP3 reply") {
    decodesAlike(Json.jsonType("j", JsonPath("$.a")), "*1\r\n$7\r\ninteger\r\n", "*1\r\n*1\r\n$7\r\ninteger\r\n", Vector(Some(JsonType.Integer)))
    decodesAlike(
      Json.jsonType("j", JsonPath("$..a")),
      "*2\r\n$7\r\ninteger\r\n$6\r\nstring\r\n",
      "*1\r\n*2\r\n$7\r\ninteger\r\n$6\r\nstring\r\n",
      Vector(Some(JsonType.Integer), Some(JsonType.String))
    )
    decodesAlike(Json.jsonType("j", JsonPath("$.missing")), "*0\r\n", "*1\r\n*0\r\n", Vector.empty)
    decodesAlike(Json.jsonType("nojson"), "$-1\r\n", "*1\r\n_\r\n", Vector(None))
    decodesAlike(Json.jsonType("j", JsonPath("$..a")), "*2\r\n+integer\r\n+string\r\n", Vector(Some(JsonType.Integer), Some(JsonType.String)))
  }

  test("JSON.STRLEN, ARRLEN, OBJLEN, OBJKEYS and DEBUG MEMORY decode Valkey's missing-key null and Redis's DEBUG MEMORY [] as no matches") {
    decodesAlike(Json.jsonStrLen("nojson"), "_\r\n", Vector.empty)
    decodesAlike(Json.jsonArrLen("nojson", JsonPath("$.x")), "_\r\n", Vector.empty)
    decodesAlike(Json.jsonObjLen("nojson"), "_\r\n", Vector.empty)
    decodesAlike(Json.jsonObjKeys("nojson", JsonPath("$.x")), "_\r\n", Vector.empty)
    decodesAlike(Json.jsonDebugMemory("nojson"), "_\r\n", Vector.empty)
    decodesAlike(Json.jsonDebugMemory("nojson"), "*0\r\n", Vector.empty)
  }

  test("JSON.NUMINCRBY decodes the RESP2 JSON-array string like the RESP3 number array") {
    decodesAlike(Json.jsonNumIncrBy("j", JsonPath("$..a"), 2.0), "$8\r\n[3,null]\r\n", "*2\r\n:3\r\n_\r\n", Vector(Some(3.0), None))
    decodesAlike(Json.jsonNumMultBy("j", JsonPath("$.e"), 2.0), "$5\r\n[3.0]\r\n", "*1\r\n,3\r\n", Vector(Some(3.0)))
    decodesAlike(Json.jsonNumIncrBy("j", JsonPath("$.a"), 1e300), "$7\r\n[1e300]\r\n", "*1\r\n,1e+300\r\n", Vector(Some(1e300)))
  }

  test("JSONPath replies with nulls for non-matching types decode alike under both protocols") {
    decodesAlike(Json.jsonStrLen("j", JsonPath("$..a")), "*2\r\n_\r\n:1\r\n", Vector(None, Some(1L)))
    decodesAlike(Json.jsonToggle("j", JsonPath("$..a")), "*2\r\n_\r\n:0\r\n", Vector(None, Some(false)))
    decodesAlike(Json.jsonObjKeys("j", JsonPath("$..a")), "*2\r\n_\r\n*1\r\n$1\r\nx\r\n", Vector(None, Some(Vector("x"))))
    decodesAlike(Json.jsonArrPop[String, String]("j", JsonPath("$.d")), "*1\r\n_\r\n", Vector(None))
    decodesAlike(Json.jsonMGet[String, String](JsonPath("$.a"))("j", "nojson"), "*2\r\n$3\r\n[1]\r\n_\r\n", Vector(Some("[1]"), None))
    decodesAlike(Json.jsonSet("j", JsonPath("$.a"), "5", JsonSetCondition.IfNotExists), "_\r\n", false)
  }
}
