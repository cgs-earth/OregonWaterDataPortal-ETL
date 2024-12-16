caddy:
	cp ./Caddyfile /etc/caddy/Caddyfile
	sudo systemctl restart caddy

start:
	source ./venv/bin/activate
	dagster dev

wis2box:
	git clone https://github.com/cgs-earth/wis2box/
	
wipedb:
	docker volume rm oregonwaterdataportal-etl_postgis_volume

test:
	pytest -vv -x

gh:
	gh act

uv:
# uv is a python version manager and venv manager that we use because of the fact that dagster is pinned to specific
# python versions and is not trivial to manage. 
	uv python install 3.12
	uv sync 
	source .venv/bin/activate

requirements:
	uv export --format requirements-txt >> requirements.txt
