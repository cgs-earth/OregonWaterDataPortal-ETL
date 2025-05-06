# this should match the .pythonversion at the root of the repo
FROM python:3.12-slim

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

COPY ../requirements.txt requirements.txt

RUN pip install -r requirements.txt

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

COPY ../dagster.yaml /opt/dagster/dagster_home/
COPY ../pyproject.toml /opt/dagster/app/pyproject.toml
COPY userCode /opt/dagster/app/userCode

WORKDIR /opt/dagster/app

EXPOSE 3000

ENV PRODUCTION=true

# use sh so that env vars are available
ENTRYPOINT ["sh", "-c", "dagster dev -h 0.0.0.0"]
