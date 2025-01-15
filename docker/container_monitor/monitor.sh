#!/bin/sh

CHANNEL="#cgs-iow-bots"

docker events --filter 'event=die' | while read event
do
  # Extract container name
  CONTAINER_NAME=$(echo "$event" | grep -oE 'name=[^,]+' | cut -d'=' -f2)
  
  if [ -n "$CONTAINER_NAME" ]; then
    MESSAGE="Container $CONTAINER_NAME exited unexpectedly."
    echo "Sending Slack notification: $MESSAGE"
    curl -X POST -H 'Content-type: application/json' \
      --data "{\"channel\":\"$CHANNEL\", \"text\":\"$MESSAGE\"}" $SLACK_HOOK_URL
  else
    echo "No container name found in the event: $event"
  fi
done
