package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/client"
)

func watchAndHandleEvents(docker *client.Client, slackAPI SlackClient, tailLength int) {
	eventsChan, errsChan := docker.Events(context.Background(), events.ListOptions{})

	for {
		select {
		case event := <-eventsChan:
			if event.Type == events.ContainerEventType && event.Action == "die" {
				containerID := event.Actor.ID
				containerName := event.Actor.Attributes["name"]
				exitCode := event.Actor.Attributes["exitCode"]

				const exitedGracefully = "143"
				if exitCode != "0" && exitCode != exitedGracefully {
					message := fmt.Sprintf("Container %s exited unexpectedly with exit code %s.", containerName, exitCode)
					logger.Print(message)

					timestampID, err := slackAPI.SendMessage(message)
					if err != nil {
						logger.Fatalf("Failed to send Slack message: %v\n", err)
					}

					// Retrieve and send container logs
					logs, err := getContainerLogs(docker, containerID)
					if err != nil {
						logger.Printf("Failed to retrieve logs for container %s: %v\n", containerName, err)
						return
					}

					logMsg := fmt.Sprintf("Last %d lines of logs for container `%s`:\n```%s```", tailLength, containerName, logs)

					timestampID, err = slackAPI.SendMessageThreaded(logMsg, timestampID)
					if err != nil {
						logger.Fatalf("Failed to send Slack message: %v\n", err)
					}

					// Send raw event JSON in the thread
					fullEventJSON, err := json.MarshalIndent(event, "", "  ")
					if err != nil {
						logger.Fatalf("Failed to marshal event to JSON: %v\n", err)
					}

					// format a markdown block
					fullEventJSON = []byte(fmt.Sprintf("Event description:\n```\n%s\n```", string(fullEventJSON)))

					_, err = slackAPI.SendMessageThreaded(string(fullEventJSON), timestampID)
					if err != nil {
						logger.Fatalf("Failed to send Slack message: %v\n", err)
					}
				}
			}
		case err := <-errsChan:
			if err != nil {
				logger.Fatalf("Error reading Docker events: %v\n", err)
			}
		}
	}
}
