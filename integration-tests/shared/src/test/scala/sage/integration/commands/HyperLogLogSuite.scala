package sage.integration.commands

import sage.integration.BothServersSuite

class HyperLogLogSuite extends BothServersSuite {

  clientTest("PFADD reports change and PFCOUNT estimates cardinality") { client =>
    client.pfAdd("hll", "a", "b", "c").is(true) >>
      client.pfAdd("hll", "a").is(false) >>
      client.pfCount("hll").is(3L) >>
      client.pfAdd[String]("hll-empty").is(true)
  }

  clientTest("PFMERGE unions HyperLogLogs and PFCOUNT spans multiple keys") { client =>
    client.pfAdd("hll-a", "a", "b", "c") >>
      client.pfAdd("hll-b", "c", "d", "e") >>
      client.pfMerge("hll-merged", "hll-a", "hll-b") >>
      client.pfCount("hll-merged").is(5L) >>
      client.pfCount("hll-a", "hll-b").is(5L)
  }
}
