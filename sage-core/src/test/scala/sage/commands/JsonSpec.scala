package sage.commands

class JsonSpec extends munit.FunSuite with BothProtocols {

  test("JSON.TYPE decodes the flat RESP2 reply like the nested Redis RESP3 reply") {
    decodesAlike(Json.jsonType("j", JsonPath("$.a")), "*1\r\n$7\r\ninteger\r\n", "*1\r\n*1\r\n$7\r\ninteger\r\n", Vector(Some(JsonType.Integer)))
    decodesAlike(
      Json.jsonType("j", JsonPath("$..a")),
      "*2\r\n$7\r\ninteger\r\n$6\r\nstring\r\n",
      "*1\r\n*2\r\n$7\r\ninteger\r\n$6\r\nstring\r\n",
      Vector(Some(JsonType.Integer), Some(JsonType.String))
    )
    decodesAlike(Json.jsonType("j", JsonPath("$.missing")), "*0\r\n", "*1\r\n*0\r\n", Vector.empty)
  }

  test("JSON.TYPE on a missing key decodes the bare null like Redis's RESP3 [null]") {
    decodesAlike(Json.jsonType("nojson"), "$-1\r\n", "*1\r\n_\r\n", Vector(None))
  }

  test("JSON.TYPE decodes the Valkey simple-string reply under both protocols") {
    decodesAlike(
      Json.jsonType("j", JsonPath("$..a")),
      "*2\r\n+integer\r\n+string\r\n",
      "*2\r\n+integer\r\n+string\r\n",
      Vector(Some(JsonType.Integer), Some(JsonType.String))
    )
    decodesAlike(Json.jsonType("nojson"), "$-1\r\n", "_\r\n", Vector(None))
  }

  test("JSON.NUMINCRBY decodes the RESP2 JSON-array string like the RESP3 number array") {
    decodesAlike(Json.jsonNumIncrBy("j", JsonPath("$..a"), 2.0), "$8\r\n[3,null]\r\n", "*2\r\n:3\r\n_\r\n", Vector(Some(3.0), None))
    decodesAlike(Json.jsonNumMultBy("j", JsonPath("$.e"), 2.0), "$5\r\n[3.0]\r\n", "*1\r\n,3\r\n", Vector(Some(3.0)))
    decodesAlike(Json.jsonNumIncrBy("j", JsonPath("$.a"), 1e300), "$7\r\n[1e300]\r\n", "*1\r\n,1e+300\r\n", Vector(Some(1e300)))
  }

  test("JSONPath replies with nulls for non-matching types decode alike under both protocols") {
    decodesAlike(Json.jsonStrLen("j", JsonPath("$..a")), "*2\r\n$-1\r\n:1\r\n", "*2\r\n_\r\n:1\r\n", Vector(None, Some(1L)))
    decodesAlike(Json.jsonToggle("j", JsonPath("$..a")), "*2\r\n$-1\r\n:0\r\n", "*2\r\n_\r\n:0\r\n", Vector(None, Some(false)))
    decodesAlike(
      Json.jsonObjKeys("j", JsonPath("$..a")),
      "*2\r\n$-1\r\n*1\r\n$1\r\nx\r\n",
      "*2\r\n_\r\n*1\r\n$1\r\nx\r\n",
      Vector(None, Some(Vector("x")))
    )
    decodesAlike(Json.jsonArrPop[String, String]("j", JsonPath("$.d")), "*1\r\n$-1\r\n", "*1\r\n_\r\n", Vector(None))
    decodesAlike(
      Json.jsonMGet[String, String](JsonPath("$.a"))("j", "nojson"),
      "*2\r\n$3\r\n[1]\r\n$-1\r\n",
      "*2\r\n$3\r\n[1]\r\n_\r\n",
      Vector(Some("[1]"), None)
    )
    decodesAlike(Json.jsonSet("j", JsonPath("$.a"), "5", JsonSetCondition.IfNotExists), "$-1\r\n", "_\r\n", false)
  }
}
