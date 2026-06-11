package sage.client.internal

import sage.client.ReadFrom
import sage.cluster.Node
import sage.commands.{Command, Execution}

class ReadRoutingSpec extends munit.FunSuite {

  private val m  = Node("master", 6379)
  private val r1 = Node("replica1", 6379)
  private val r2 = Node("replica2", 6379)

  test("Master routes to the master only") {
    assertEquals(ReadRouting.candidates(ReadFrom.Master, m, Vector(r1, r2), 0), Vector(m))
  }

  test("MasterPreferred tries the master first, then replicas") {
    assertEquals(ReadRouting.candidates(ReadFrom.MasterPreferred, m, Vector(r1, r2), 0), Vector(m, r1, r2))
  }

  test("ReplicaPreferred tries replicas first, then the master") {
    assertEquals(ReadRouting.candidates(ReadFrom.ReplicaPreferred, m, Vector(r1, r2), 0), Vector(r1, r2, m))
  }

  test("Replica lists replicas only, round-robin-rotated by the cursor") {
    assertEquals(ReadRouting.candidates(ReadFrom.Replica, m, Vector(r1, r2), 0), Vector(r1, r2))
    assertEquals(ReadRouting.candidates(ReadFrom.Replica, m, Vector(r1, r2), 1), Vector(r2, r1))
    assertEquals(ReadRouting.candidates(ReadFrom.Replica, m, Vector(r1, r2), 2), Vector(r1, r2))
  }

  test("Replica with no replicas is empty, so the strict policy fails") {
    assertEquals(ReadRouting.candidates(ReadFrom.Replica, m, Vector.empty, 0), Vector.empty)
  }

  test("ReplicaPreferred with no replicas falls back to the master") {
    assertEquals(ReadRouting.candidates(ReadFrom.ReplicaPreferred, m, Vector.empty, 0), Vector(m))
  }

  test("a negative cursor still rotates within bounds") {
    assertEquals(ReadRouting.candidates(ReadFrom.Replica, m, Vector(r1, r2), -1), Vector(r2, r1))
  }

  private def cmd(
    name: String,
    execution: Execution = Execution.Ordinary,
    isReadOnly: Boolean = false,
    cursorBound: Boolean = false
  ): Command[Unit] =
    Command(name, Command.NoKeys, Vector.empty, _ => Right(()), execution, isReadOnly = isReadOnly, cursorBound = cursorBound)

  test("eligibility: ordinary read-only is eligible; writes, blocking reads, and cursor-bound scans are not") {
    assert(ReadRouting.replicaEligible(cmd("GET", isReadOnly = true)))
    assert(!ReadRouting.replicaEligible(cmd("SET")))
    assert(!ReadRouting.replicaEligible(cmd("XREAD", Execution.Blocking, isReadOnly = true)))
    // a SCAN cursor is only valid on its issuing node, so it must never round-robin across replicas
    assert(!ReadRouting.replicaEligible(cmd("HSCAN", isReadOnly = true, cursorBound = true)))
  }
}
