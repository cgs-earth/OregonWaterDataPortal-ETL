package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/log"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/slack-go/slack"
)

// Configuration needed to send a slack message
type SlackConfig struct {
	SlackToken       string
	SlackChannelName string
	SlackBotName     string
	SlackBotAvatar   string
}

// A wrapper client that contains both the client and the necessary configuration to send th emessage
type SlackClient struct {
	API    *slack.Client
	Config SlackConfig
}

// Create a new slack client
func NewSlackClient() *SlackClient {
	slackConfig := SlackConfig{
		SlackToken:       strictEnv("SLACK_BOT_TOKEN"),
		SlackChannelName: strictEnv("SLACK_CHANNEL_NAME"),
		SlackBotName:     strictEnv("SLACK_BOT_NAME"),
		SlackBotAvatar:   strictEnv("SLACK_BOT_AVATAR"),
	}
	api := slack.New(slackConfig.SlackToken)
	return &SlackClient{API: api, Config: slackConfig}
}

// Send a message to a channel on slack and return the thread in question
// All text is sent as markdown
func (s *SlackClient) SendMessage(message string, opts ...slack.MsgOption) (string, error) {
	log.Infof("Sending message to channel %s: %s", s.Config.SlackChannelName, message)
	postParams := slack.PostMessageParameters{
		Username: s.Config.SlackBotName,
		IconURL:  s.Config.SlackBotAvatar,
		Markdown: true,
	}
	_, timestampIdentifier, err := s.API.PostMessage(
		s.Config.SlackChannelName,
		append(
			[]slack.MsgOption{
				slack.MsgOptionText(message, false),
				slack.MsgOptionUsername(s.Config.SlackBotName),
				slack.MsgOptionIconURL(s.Config.SlackBotAvatar),
				slack.MsgOptionPostMessageParameters(postParams),
			},
			opts...,
		)...,
	)
	if err != nil {
		return "", err
	}
	return timestampIdentifier, nil
}

// Send a message to a thread by specifying a thread timestamp.
// Returns the timestamp identifier of the sent message so another
// threaded message can be sent to the same thread.
func (s *SlackClient) SendMessageThreaded(message, threadTimestamp string) (string, error) {
	timestampIdentifier, err := s.SendMessage(message, slack.MsgOptionTS(threadTimestamp))
	return timestampIdentifier, err
}

// Get the value of an environment variable or panic if it is not set
func strictEnv(envVarName string) string {
	envVal := os.Getenv(envVarName)
	if envVal == "" {
		log.Fatalf("Error: Missing required environment variable %s.", envVarName)
	}
	return envVal
}

// Get the logs for a container as a newline separated string
func getContainerLogs(cli *client.Client, containerID string) (string, error) {
	reader, err := cli.ContainerLogs(context.Background(), containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       fmt.Sprintf("%d", tailLength),
	})
	if err != nil {
		return "", err
	}
	defer reader.Close()

	var logs strings.Builder
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		logs.WriteString(scanner.Text() + "\n")
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	return logs.String(), nil
}
