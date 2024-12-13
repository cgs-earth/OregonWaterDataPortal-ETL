Integration for ingesting data from Oregon APIs and exposing it as sensorthings data

# How to use

Spin up the infrastructure with docker
```sh
docker compose up

```

You then need to start Caddy if you want to get https in production.

To run dagster you need to run `dagster dev` after installing the python dependencies. This project is set up to use `uv` as the package manager.