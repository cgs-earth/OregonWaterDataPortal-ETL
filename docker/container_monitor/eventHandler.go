package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/charmbracelet/log"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/client"
)

func watchAndHandleEvents(docker *client.Client, slackAPI SlackClient, tailLength int) {
	eventsChan, errsChan := docker.Events(context.Background(), events.ListOptions{})

	for {
		select {
		case event := <-eventsChan:
			if event.Type == events.ContainerEventType && event.Action == "die" {
				log.Debug("Received container die event")

				containerID := event.Actor.ID
				containerName := event.Actor.Attributes["name"]
				exitCode := event.Actor.Attributes["exitCode"]

				const exitedGracefully = "143"
				if exitCode == "0" || exitCode == exitedGracefully {
					log.Infof("Container %s exited gracefully with exit code %s. No notification will be sent", containerName, exitCode)
					continue
				}

				message := fmt.Sprintf("Container %s exited unexpectedly with exit code %s.", containerName, exitCode)
				log.Infof(message)

				timestampID, err := slackAPI.SendMessage(message)
				if err != nil {
					log.Errorf("Failed to send 1 Slack message: %v\n", err)
					continue
				}

				// Retrieve and send container logs
				logs, err := getContainerLogs(docker, containerID)
				if err != nil {
					log.Errorf("Failed to retrieve logs for container %s: %v\n", containerName, err)
					continue
				}

				logMsg := fmt.Sprintf("Last %d lines of logs for container `%s`:\n```%s```", tailLength, containerName, logs)

				timestampID, err = slackAPI.SendMessageThreaded(logMsg, timestampID)
				if err != nil {
					log.Errorf("Failed to send Slack message: %v\n", err)
					continue
				}

				// Send raw event JSON in the thread
				fullEventJSON, err := json.MarshalIndent(event, "", "  ")
				if err != nil {
					log.Errorf("Failed to marshal event to JSON: %v\n", err)
					continue
				}

				// format a markdown block
				fullEventJSON = fmt.Appendf(nil, "Event description:\n```\n%s\n```", string(fullEventJSON))

				_, err = slackAPI.SendMessageThreaded(string(fullEventJSON), timestampID)
				if err != nil {
					log.Errorf("Failed to send Slack message: %v\n", err)
					continue
				}
			}
		case err := <-errsChan:
			if err != nil {
				log.Errorf("Error reading Docker events: %v\n", err)
			}
		}
	}
}
