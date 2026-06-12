# Configuration

<!--
WRITER NOTES — replace this body with real content.
Ground snippets in:
  examples/zio/ClusterExample.scala         (cluster + sharded pub/sub)
  examples/ox/MasterReplicaExample.scala    (master-replica + ReadFrom)
  examples/ce/TlsExample.scala              (TLS + ACL)
Key point from examples/README.md: these are CONFIG-ONLY differences — the command
code is identical to the tours. Lead with that: same Client type, SageConfig selects the
topology and runtime.

Purpose: everything you set on SageConfig — topology (standalone / cluster /
master-replica), read routing, database selection, and secure auth. Frame the whole
page as "config, not code": the command surface never changes.

Sections to cover (grounded in CONTEXT.md glossary):

1. One Client, any topology — standalone / cluster / master-replica are all the same
   `SageClient` type; only configuration (Topology) selects the runtime. Frame the
   whole page as "config, not code".

2. Standalone — the default; mention Database selection (SELECT 0–15, fixed for the
   Client's lifetime, re-applied on reconnect; a cluster has only DB 0).

3. Cluster — Seeds bootstrap topology discovery; routing by Slot (CRC16, hash tags);
   MOVED/ASK redirects handled transparently. Source: ClusterExample.scala.

4. Master-Replica — one master + replicas discovered from seeds (Topology.MasterReplica);
   no slots, no redirects, no cluster bus. Source: MasterReplicaExample.scala.

5. ReadFrom (Read Policy) — Master (default) / MasterPreferred / Replica /
   ReplicaPreferred; same setting for cluster AND master-replica. Only read-only
   commands are eligible; writes always go to the master. `Replica` fails with no
   replica; `*Preferred` falls back to master. Replica reads may lag — accepted contract.

6. TLS & ACL — secure transport + username/password auth at connection setup.
   Source: TlsExample.scala. Don't dump cert/trust internals; show the config shape.
-->
