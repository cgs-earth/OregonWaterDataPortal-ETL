# Oregon Water Data Portal ETL

The Oregon Water Data portal crawls or ingests data from multiple different Oregon sources and exposes them via a Sensorthings and OGC API Features endpoint.

# How to use

1. Spin up the infrastructure with docker

```sh
docker compose --env-file owdp.env up
```

Access the frontend at `localhost:8999` and the API at `localhost:8999/oapi`,
or edit `owdp.env` and deploy at your own URL.

2. Start Caddy with `make caddy` if you want to get https in production.

3. Run dagster with `dagster dev`
   - You must use python 3.12 or below since 3.13 is not supported by Dagster currently
   - This project uses `uv` since it makes it easier to manage the python version
