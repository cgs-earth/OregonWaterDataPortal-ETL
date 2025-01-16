package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type LogWatcherConfig struct {
	Patterns []string // Patterns to watch in the logs
}

// Watch a container's logs for specific patterns and send a Slack message when a pattern is matched
func watchAndHandleLogs(docker *client.Client, slackAPI SlackClient, containerName string, logWatcherConfig LogWatcherConfig) {
	logger.Printf("Listening for logs for container %s...\n", containerName)

	ctx := context.Background()

	nowTime := time.Now()

	// Retrieve container logs as a stream
	logs, err := docker.ContainerLogs(ctx, containerName, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Timestamps: true,
		Since:      nowTime.Format(time.RFC3339),
	})
	if err != nil {
		log.Fatalf("Failed to retrieve logs for container %s: %v\n", containerName, err)
	}
	defer logs.Close()

	// Use a scanner to read the logs line by line
	scanner := bufio.NewScanner(logs)
	for scanner.Scan() {
		logLine := scanner.Text()
		for _, pattern := range logWatcherConfig.Patterns {
			if strings.Contains(logLine, pattern) {
				message := fmt.Sprintf("Container `%s`: matched watch pattern `%s` with log line: \n`%s`", containerName, pattern, logLine)
				log.Printf("Sending Slack notification to channel %s with message: %s", slackAPI.Config.SlackChannelName, message)

				_, err := slackAPI.SendMessage(message)
				if err != nil {
					log.Fatalf("Failed to send Slack message: %v\n", err)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading container logs: %v\n", err)
	}
}
