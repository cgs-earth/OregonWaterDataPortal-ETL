package main

import (
	"strings"

	"github.com/charmbracelet/log"
	"github.com/docker/docker/client"
)

const tailLength = 20

func main() {
	log.SetLevel(log.DebugLevel)
	slackClient := NewSlackClient()
	docker, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("Error creating Docker client: %v", err)
	}
	defer docker.Close()

	log.Info("Listening for Docker events...")

	go func() {
		watchAndHandleEvents(docker, *slackClient, tailLength)
	}()

	go func() {
		watchAndHandleLogs(docker, *slackClient, strictEnv("LOG_WATCH_CONTAINER_NAME"), LogWatcherConfig{
			Patterns: strings.Split(strictEnv("LOG_WATCH_COMMA_SEPARATED_PATTERNS"), ","),
		})
	}()
	// Block forever
	select {}
}
