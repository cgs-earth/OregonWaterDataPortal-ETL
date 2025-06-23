#!/bin/bash
# Simplified pygeoapi entrypoint with hot reload on template folder

echo "START /entrypoint.sh"

set +e

export PYGEOAPI_HOME=/pygeoapi
export PYGEOAPI_CONFIG="${PYGEOAPI_HOME}/local.config.yml"
export PYGEOAPI_OPENAPI="${PYGEOAPI_HOME}/local.openapi.yml"
export SCRIPT_NAME=${SCRIPT_NAME:=/}
export CONTAINER_NAME=${CONTAINER_NAME:=pygeoapi}
export CONTAINER_HOST=${CONTAINER_HOST:=0.0.0.0}
export CONTAINER_PORT=${CONTAINER_PORT:=80}
export WSGI_APP=${WSGI_APP:=pygeoapi.flask_app:APP}
export WSGI_WORKERS=${WSGI_WORKERS:=4}
export WSGI_WORKER_TIMEOUT=${WSGI_WORKER_TIMEOUT:=6000}
export WSGI_WORKER_CLASS=${WSGI_WORKER_CLASS:=gevent}

cd ${PYGEOAPI_HOME}

echo "Generating openapi.yml..."
pygeoapi openapi generate ${PYGEOAPI_CONFIG} --output-file ${PYGEOAPI_OPENAPI}
[[ $? -ne 0 ]] && { echo "ERROR: Failed to generate openapi.yml"; exit 1; }

# Lock .py files to avoid unwanted reloads
find . -type f -name "*.py" | xargs chmod 0444

# Watch template folder for hot reload
WATCH_DIR="/pygeoapi/pygeoapi/templates/jsonld/things"
RELOAD_FILES=$(find "$WATCH_DIR" -type f)

RELOAD_ARGS=""
for file in $RELOAD_FILES; do
  RELOAD_ARGS+=" --reload-extra-file $file"
done

# Adjust SCRIPT_NAME if necessary
[[ "${SCRIPT_NAME}" = '/' ]] && export SCRIPT_NAME=""

echo "Starting gunicorn with hot reload on ${CONTAINER_HOST}:${CONTAINER_PORT}"
exec gunicorn --reload \
  --workers ${WSGI_WORKERS} \
  --worker-class=${WSGI_WORKER_CLASS} \
  --timeout ${WSGI_WORKER_TIMEOUT} \
  --name=${CONTAINER_NAME} \
  --bind ${CONTAINER_HOST}:${CONTAINER_PORT} \
  --reload-extra-file ${PYGEOAPI_CONFIG} \
  $RELOAD_ARGS \
  ${WSGI_APP}
