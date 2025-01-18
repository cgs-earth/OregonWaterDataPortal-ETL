package main

import (
	"log"
	"os"
	"strings"

	"github.com/docker/docker/client"
)

const tailLength = 20

var logger *log.Logger

func main() {
	logger = log.New(os.Stdout, "", log.LstdFlags)

	slackClient := NewSlackClient()
	docker, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		logger.Fatalf("Error creating Docker client: %v\n", err)
	}
	defer docker.Close()

	logger.Print("Listening for Docker events...\n")

	go func() {
		watchAndHandleEvents(docker, *slackClient, tailLength)
	}()

	go func() {
		patterns := strictEnv("LOG_WATCH_COMMA_SEPARATED_PATTERNS")

		patternList := strings.Split(patterns, ",")

		watchAndHandleLogs(docker, *slackClient, strictEnv("LOG_WATCH_CONTAINER_NAME"), LogWatcherConfig{
			Patterns: patternList,
		})
	}()
	// Block forever 
	select {}
}
