# Oregon Water Data Portal ETL

The Oregon Water Data portal crawls or ingests data from multiple different Oregon sources and exposes them via a Sensorthings and OGC API Features endpoint.

## Usage

Copy the `.env.example` file to `.env` and change the URL to your desired domain.

### Development

1. Install [uv](https://github.com/astral-sh/uv)
2. Enter the virtual environment
3. Run the user code locally with the following command

```sh
make dev
```

Services can be accessed at the following URLs:

| Service    | URL                                 |
| ---------- | ----------------------------------- |
| Frontend   | http://localhost:8999               |
| Crawler UI | http://localhost:3000               |
| API        | http://localhost:8999/oapi          |
| Database   | http://localhost:8999/FROST-Server/ |

### Production

_Builds the images then run user code as a container_

```
make prodBuild
make prodUp
```

_Start the reverse proxy, Caddy, which exposes the services and provisions the SSL certificate_

```
make caddy
```

## Performance tips

- Once you start the database and the frost server, you will likely want to add extra indices to optimize queries.
  - Run `make addIndices` to add them

## Integrations

Each integration that we are ingesting is defined as a separate folder in the [userCode](userCode/) folder.

Documentation for all individual integrations can be found in the [docs](docs/) folder
