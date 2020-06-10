package main

import (
	"log"
	"net"
	"os"
	"fmt"
	"strings"

	"github.com/infrawatch/apputils/connector"
	"github.com/infrawatch/apputils/logging"
	"github.com/infrawatch/apputils/config"
)

func main() {
		logger, err := logging.NewLogger(logging.DEBUG, "/dev/stderr")
	if err != nil {
		fmt.Println("Failed to initialize logger")
		os.Exit(2)
	}
	defer logger.Destroy()

	metadata := map[string][]config.Parameter{
		"amqp1": []config.Parameter{
			config.Parameter{Name: "connection", Tag: "", Default: "amqp://localhost:5672/lokean/logs", Validators: []config.Validator{}},
			config.Parameter{Name: "send_timeout", Tag: "", Default: 2, Validators: []config.Validator{config.IntValidatorFactory()}},
			config.Parameter{Name: "client_name", Tag: "", Default: "test", Validators: []config.Validator{}},
		},
	}
	conf := config.NewINIConfig(metadata, logger)
	err = conf.Parse("config.ini")
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Error while parsing config file")
		return
	}

	amqp, err := connector.NewAMQP10Connector(conf, logger)
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Couldn't connect to AMQP")
		return
	}
	err = amqp.Connect()
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Error while connecting to AMQP")
		return
	}
	amqpReceiver := make(chan interface{})
	amqpSender := make(chan interface{})
	amqp.Start(amqpReceiver, amqpSender)

	// Listen on TCP port 2000 on all available unicast and
	// anycast IP addresses of the local system.
	l, err := net.Listen("tcp", ":2000")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c net.Conn) {
			msg := make([]byte, 100000)
			_, err := c.Read(msg)
			if err != nil {
				logger.Error("Error while reading messages from socket")
				logger.Error("err")
			}
			strs := strings.Split(string(msg), "\n")
			for index, str := range strs {
				// The last str contains the unused part of msg buffer (it's empty)
				if index != len(strs) -1 {
					amqpSender <- connector.AMQP10Message{Address: "lokean/logs", Body: str}
				}
			}
			c.Close()
		}(conn)
	}
}
