# start the reverse proxy which gives our server https and points to the proper domain
caddy:
	cp ./Caddyfile /etc/caddy/Caddyfile
	sudo systemctl restart caddy

# start dagster. Requires the docker compose to be running to access the infrastructure
start:
	source ./venv/bin/activate
	dagster dev

# get rid of the sensorthings db, mainly for testing purposes
wipedb:
	docker volume rm oregonwaterdataportal-etl_postgis_volume

# run tests on the dagster pipeline. NOTE: this will clear the db and start fresh
test:
	pytest -vv -x

# install uv for python package management
uv:
# uv is a python version manager and venv manager that we use because of the fact that dagster is pinned to specific
# python versions and is not trivial to manage. 
	wget -qO- https://astral.sh/uv/install.sh | sh

# install requirements needed to run dagster
build:
# make sure you are in a new shell env after installing uv
	uv python install 3.12
	uv sync 
	source .venv/bin/activate
