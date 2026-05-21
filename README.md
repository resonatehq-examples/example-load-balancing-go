<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/banner-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="./assets/banner-light.png">
    <img alt="Worker load balancing — Resonate Go SDK" src="./assets/banner-dark.png">
  </picture>
</p>

# Worker load balancing | Resonate Go SDK

Service discovery and automatic load balancing across multiple Resonate workers — in a single Go binary.

> Heads up — `resonate-sdk-go` is pre-release. The SDK has no semver tag yet, so this example pins to a specific commit. Expect API changes until `v0.1.0`.

## The problem

A single worker instance will eventually be overwhelmed under load. The conventional answer is horizontal scaling — run more instances. But that introduces service discovery and load balancing: knowing which worker has capacity, routing tasks to it, and recovering work if a worker crashes mid-execution.

These are distributed systems concerns that developers are typically forced to weave into application logic.

## The solution

Resonate has built-in service discovery, load balancing, and recovery. Workers declare a group; the client targets `poll://any@<group>`. The server handles the rest.

```go
// Worker: join the "workers" group
r, err := resonate.New(resonate.Config{
    Network: httpnet.NewHTTP(serverURL, httpnet.HTTPOptions{
        PID:   "worker-1",  // unique per instance
        Group: "workers",   // shared group name
    }),
})

// Client: target any worker in the group
h, err := client.RPC(ctx, id, "computeSomething", args,
    resonate.RPCOptions{Target: "poll://any@workers"},
)
```

Run as many instances as needed. Resonate distributes tasks across all of them.

## About this example

This example starts N worker goroutines inside one Go binary, each constructing its own `*resonate.Resonate` with a unique PID and the same worker group. A client goroutine dispatches M tasks and prints which worker handled each one.

In production you would run each worker as a separate OS process or container — the Go SDK does not require that; the server's group-routing works identically.

## Why a real server is required

**localnet does not support multi-worker load balancing.**

`localnet.NewLocal` creates a fully self-contained in-process server. Each `resonate.New` call with a distinct `LocalNetwork` has its own isolated server state. The execute-message dispatch and anycast routing both happen within each instance's own actor goroutine — there is no shared bus between multiple `LocalNetwork` instances. The real `resonate dev` server provides that shared bus via its SSE long-poll endpoint at `/poll/{group}/{pid}`, which all workers connect to concurrently.

**This is an important limitation to be aware of** when writing tests: unit tests that use localnet are single-worker only. Multi-worker scenarios require a real server or an equivalent shared-state mock.

## Prerequisites

- Go 1.22+
- The `resonate` server CLI. Install with Homebrew on macOS or Linux:
  ```
  brew install resonatehq/tap/resonate
  ```
  Other install paths: <https://docs.resonatehq.io/get-started/install>.

## Setup

```sh
git clone https://github.com/resonatehq-examples/example-load-balancing-go.git
cd example-load-balancing-go
go mod download
```

## Run it

In one terminal, start the dev server:

```sh
resonate dev
```

In another, run the example:

```sh
go run . -url=http://localhost:8001
```

Use flags to control the demo:

```sh
go run . -url=http://localhost:8001 -workers=5 -tasks=15
```

| Flag | Default | Description |
|------|---------|-------------|
| `-url` | *(required)* | Resonate server URL |
| `-workers` | `3` | Number of worker instances to start |
| `-tasks` | `6` | Number of tasks to dispatch |
| `-group` | `workers` | Worker group name |

## What to look for

Expected output (task-to-worker assignment varies run to run):

```
[worker-1] started (pid=worker-1-..., group=workers)
[worker-2] started (pid=worker-2-..., group=workers)
[worker-3] started (pid=worker-3-..., group=workers)
[client] dispatching task-0
[client] dispatching task-1
[client] dispatching task-2
[client] dispatching task-3
[client] dispatching task-4
[client] dispatching task-5

[client] waiting for 6 tasks to complete...

[worker-2] handling task-0 → done
[worker-1] handling task-1 → done
[worker-3] handling task-2 → done
[worker-2] handling task-3 → done
[worker-1] handling task-4 → done
[worker-3] handling task-5 → done
[client] task-0 → "worker-2 handled task-0"
[client] task-1 → "worker-1 handled task-1"
...
Done. 6/6 tasks completed successfully.
```

Tasks spread across workers automatically. No round-robin logic in the application — the Resonate server handles routing.

You can inspect the durable promises on the dashboard at <http://localhost:8001>.

## File structure

```
example-load-balancing-go/
├── main.go        worker instances + client dispatch
├── go.mod         module declaration + SDK pin
├── go.sum         checksums
├── assets/        README banner images
├── LICENSE        Apache-2.0
└── README.md
```

## Next steps

- [Get started](https://docs.resonatehq.io/get-started) — install paths + first-program walkthrough.
- [Worker groups and targets](https://docs.resonatehq.io/concepts/targets) — how `poll://any@<group>` routing works.
- [Durable execution concepts](https://docs.resonatehq.io/concepts) — what makes task dispatch durable and how crashed workers recover.
- [`example-hello-world-go`](https://github.com/resonatehq-examples/example-hello-world-go) — simpler starting point if this is your first Resonate program.

## Community

- Discord: <https://resonatehq.io/discord>
- X: <https://x.com/resonatehqio>
- LinkedIn: <https://linkedin.com/company/resonatehq>
- YouTube: <https://youtube.com/@resonatehq>
- Journal: <https://journal.resonatehq.io>

## License

[Apache-2.0](./LICENSE)
