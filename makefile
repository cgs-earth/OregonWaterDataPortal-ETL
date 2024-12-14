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