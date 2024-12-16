Integration for ingesting data from Oregon APIs and exposing it as sensorthings data

# How to use

1. Spin up the infrastructure with docker

```sh
docker compose up
```

Access the frontend at `localhost:8999` and the API at `localhost:8999/oapi`

2. Start Caddy with `make caddy` if you want to get https in production.

3. Run dagster with `dagster dev`
   - You must use python 3.12 or below since 3.13 is not supported by Dagster currently
   - This project uses `uv` since it makes it easier to manage the python version
