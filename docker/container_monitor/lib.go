package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/slack-go/slack"
)

type SlackConfig struct {
	SlackToken       string
	SlackChannelName string
	SlackBotName     string
	SlackBotAvatar   string
}

type SlackClient struct {
	API    *slack.Client
	Config SlackConfig
}

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
	logger.Printf("Sending message to channel %s: %s", s.Config.SlackChannelName, message)
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

func strictEnv(envVarName string) string {
	envVal := os.Getenv(envVarName)
	if envVal == "" {
		log.Fatalf("Error: Missing required environment variable %s.", envVarName)
	}
	return envVal
}

func getContainerLogs(cli *client.Client, containerID string) (string, error) {
	ctx := context.Background()
	reader, err := cli.ContainerLogs(ctx, containerID, container.LogsOptions{
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
