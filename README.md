# playground

Messing about with tools.

## Run

```
docker compose up -d
go run . postgres all
```

## Tools

```
go run . graphql all       nothing needed
go run . grpc all          nothing needed
go run . kafka all         needs kafka
go run . postgres all      needs postgres
```

`go run . <tool> --help` lists the demos. `--dsn` overrides the connection.
