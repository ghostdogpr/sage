# Topology owns the server address

The server address lives inside `Topology`, not at the top of `SageConfig`. `Topology.Standalone(endpoint: Endpoint = Endpoint())` carries the one standalone address; `Topology.Cluster(seeds)` carries the seeds. `SageConfig` no longer has `host`/`port` fields. This replaces the earlier split where standalone read top-level `host`/`port` while cluster read `seeds` and ignored the top-level pair — a shape with two homes for an address and a footgun (set `host`, switch to `Cluster`, and the host is silently unused).

The cost is ergonomic: the common custom-host standalone case goes from `SageConfig(host = "h", port = 6380)` to `SageConfig(topology = Topology.Standalone(Endpoint("h", 6380)))`. The zero-arg `SageConfig()` still connects to `localhost:6379`, because `Endpoint` defaults to `("localhost", 6379)` and `Standalone` defaults to `Endpoint()`. We accepted the verbosity rather than reintroduce a second address home, and rejected full `SageConfig.standalone(...)`/`.cluster(...)` smart constructors because they would duplicate the ~15 tuning fields twice. This is the only source-breaking change in the batch and was taken pre-release, when the cost is lowest.

ADR-0007 (one client type, topology is configuration) is unchanged and in fact better served: every topology-specific input now lives in the `Topology` value, so the rest of `SageConfig` is topology-agnostic.

## Consequences

`Endpoint` is now the single address type, used for both the standalone address and each cluster seed. Validation checks the standalone endpoint's port the same way it checks each seed's. A `redis://` connection URI (`SageConfig.fromUri`) maps a single host to `Standalone` and comma-separated hosts to `Cluster` seeds, so the URI and the programmatic shape agree.

Unix domain socket addresses are **deferred**: `Endpoint` remains host/port only. Adding a socket-path endpoint form touches the transport layer (`SocketTransport`/`UnixDomainSocketAddress`) and its TLS interplay, which is beyond a config reshape. When demand appears (sidecar/same-host setups), `Endpoint` is the natural place to add a path variant.
