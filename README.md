# Oregon Water Data Portal ETL

The Oregon Water Data portal crawls or ingests data from multiple different Oregon sources and exposes them via a Sensorthings and OGC API Features endpoint.

# How to use

1. Copy the `.env.example` file to `.env` and change the URL to your desired domain.
2. Spin up the infrastructure with docker

```sh
docker compose --env-file .env up
```

3. Once you start the database and the frost server, you will likely want to add extra indices to make it faster. To do this run the [init script](./docker/wait-for-frost-and-init-db.sh)

Access the frontend at `localhost:8999` and the API at `localhost:8999/oapi`, or edit `.env` and deploy at your own URL.

4. Start Caddy with `make caddy` if you want to get https in production.
5. Run dagster with `dagster dev`
   - You must use python 3.12 or below since 3.13 is not supported by Dagster currently
   - This project uses `uv` since it makes it easier to manage the python version
   - It is helpful to use `screen` to start a background shell, run `dagster dev` then detach the screen. You can then reattach to it at a later time with `screen -r` so it runs in the background but is still accessible via the cli.
