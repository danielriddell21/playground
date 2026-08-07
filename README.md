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

`mix graph` needs postgres 19 and skips itself on anything older.

Some `lang` demos are gated and say so when you run them:

```
GOEXPERIMENT=runtimesecret go run . lang secret
GOTOOLCHAIN=go1.27rc1 go run . lang modern
```
