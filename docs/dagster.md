# Dagster

This project uses Dagster for crawling.

- Crawl schedules can be managed from the `Automation` tab
- Crawl jobs can be started and monitored from the `Jobs` tab
- Individual assets in the pipeline can be found in the `Assets` tab

Generally the deployment follows all standard Dagster best practices and thus the Dagster docs is a good place to reference for operating the dagster pipeline.

## New Integrations

To add a new integration you should create a new folder in the `userCode` directory, add the necessary assets, then add any new jobs to the [definitions](../userCode/__init__.py)
