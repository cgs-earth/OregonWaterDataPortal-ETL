package main

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/log"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type LogWatcherConfig struct {
	Patterns []string // Patterns to watch in the logs
}

func watchAndHandleLogs(docker *client.Client, slackAPI SlackClient, containerName string, logWatcherConfig LogWatcherConfig) {
	log.Infof("Starting log watcher for container %s with patterns %v", containerName, logWatcherConfig.Patterns)

	for {
		nowTime := time.Now()

		logs, err := docker.ContainerLogs(context.Background(), containerName, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
			Timestamps: true,
			Since:      nowTime.Format(time.RFC3339),
		})
		if err != nil {
			log.Errorf("Failed to retrieve logs for container %s: %v", containerName, err)
			time.Sleep(5 * time.Second) // wait before retrying
			continue
		}

		scanner := bufio.NewScanner(logs)
		for scanner.Scan() {
			logLine := scanner.Text()
			for _, pattern := range logWatcherConfig.Patterns {
				if strings.Contains(logLine, pattern) {
					message := fmt.Sprintf("Container `%s`: matched watch pattern `%s` with log line: \n`%s`", containerName, pattern, logLine)
					log.Infof("Sending Slack notification to channel %s with message: %s", slackAPI.Config.SlackChannelName, message)

					if _, err := slackAPI.SendMessage(message); err != nil {
						log.Errorf("Failed to send Slack message: %v", err)
					}
				}
			}
		}

		if err := scanner.Err(); err != nil {
			log.Errorf("Error reading container logs: %v", err)
		}

		logs.Close()
		log.Warnf("Log stream for container %s ended, reconnecting...", containerName)
		time.Sleep(2 * time.Second) // brief pause before retrying
	}
}
