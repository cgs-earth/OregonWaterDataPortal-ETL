caddy:
	cp ./Caddyfile /etc/caddy/Caddyfile
	sudo systemctl restart caddy

start:
	python3 -m venv ./venv
	source ./venv/bin/activate
	dagster dev

wipedb:
	docker volume rm oregonwaterdataportal-etl_postgis_volume

test:
	pytest -vv -x

gh:
	gh act
