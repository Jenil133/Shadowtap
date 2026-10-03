// Command fakekafka runs an in-memory, Kafka-protocol-compatible broker
// (franz-go's kfake) for local runs that cannot use Docker. Data is lost when
// it exits.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

func main() {
	port := flag.Int("port", 29092, "port to listen on")
	topic := flag.String("topic", capture.DefaultTopic, "topic to create")
	partitions := flag.Int("partitions", 6, "partitions of the topic")
	flag.Parse()

	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.Ports(*port),
		kfake.SeedTopics(int32(*partitions), *topic),
	)
	if err != nil {
		log.Fatalf("fakekafka: %v", err)
	}
	defer c.Close()
	fmt.Printf("fakekafka: listening on %v, topic %s (%d partitions)\n", c.ListenAddrs(), *topic, *partitions)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
}
