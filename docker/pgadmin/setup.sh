#!/bin/bash

# Wait for pgAdmin to start
until [ -f "/var/lib/pgadmin/pgadmin4.db" ]; do
  echo "Waiting for pgAdmin to initialize..."
  sleep 2
done

# Register the server using pgAdmin's internal CLI
python3 /pgadmin4/setup.py --load-servers /pgadmin4/servers.json --user admin@admin.com
