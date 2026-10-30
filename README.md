# playground

Messing about with tools.

## Run

```
docker compose up -d
go run . postgres all
```

Or put the backing services in kubernetes instead of compose:

```
kubectl apply -f deploy/k8s
```

Node ports are 30432 postgres, 30092 kafka, 30379 redis.

## Tools

```
go run . bigdata all       needs postgres
go run . dapr all          needs redis + daprd
go run . graphql all       nothing needed
go run . grpc all          nothing needed
go run . kafka all         needs kafka
go run . lang all          nothing needed
go run . mix all           two or more of the above together
go run . postgres all      needs postgres
go run . sqlc all          needs postgres + sqlc cli
go run . openfga all       needs the openfga binary
go run . melange all       needs postgres + melange cli
```

`go run . <tool> --help` lists the demos. `--dsn` overrides the connection.

`bigdata` takes `--rows 1m|2m|5m` (default 1m). Each scenario sends the same rows
through grpc into or out of postgres three good ways, checks all three got the
same answer, and prints how long each took:

```
rocket    in      copy off the stream, parallel copies, acked unnest batches
ufo       in      dedup via on conflict, merge, pipelined upserts
heist     out     one streamed query, tid range scans, binary copy
wrapped   out     parallel hash agg, covering index ranges, materialized view
steps     in+out  copy then group by, running totals, list partitions
coffee    both    bidi insert returning, copy then set based, unary fanout
fraud     both    card state per batch, one window, staged server pipeline
```

`mix graph` needs postgres 19 and skips itself on anything older.

Some `lang` demos are gated and say so when you run them:

```
GOEXPERIMENT=runtimesecret go run . lang secret
GOTOOLCHAIN=go1.27rc1 go run . lang modern
```
