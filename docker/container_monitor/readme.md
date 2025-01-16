# Container Monitor

Container monitor is a simple script designed to watch for errors:

1. Docker `die` events
- These events are emitted whenever a container is stopped. We then check the exit code and send a message to slack if the exit code indicated an error.
2. Log error patterns
- You can specify a particular container name and multiple error strings in one comma separated string. So for instance, `Slow Query,Error,error` would trigger the bot message on `Slow query 2000ms` or `IO Error`, among others.

## env variables 

This service can be customized by setting the following env vars. These must be set, if not, the service will fail to start.

```
- SLACK_BOT_TOKEN
- SLACK_CHANNEL_NAME
- SLACK_BOT_NAME
- SLACK_BOT_AVATAR
- LOG_WATCH_COMMA_SEPARATED_PATTERNS
- LOG_WATCH_CONTAINER
```
