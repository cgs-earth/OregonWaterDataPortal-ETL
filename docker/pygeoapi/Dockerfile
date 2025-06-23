FROM geopython/pygeoapi:latest

COPY pygeoapi.config.yml /pygeoapi/local.config.yml
COPY ./templates /pygeoapi/pygeoapi/templates
COPY ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT [ "/entrypoint.sh" ]