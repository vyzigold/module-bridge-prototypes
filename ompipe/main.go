package main

import (
	"os"
	"fmt"
	"bytes"
	"syscall"
	"io"

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

	for {
		pipe, err := os.OpenFile("/tmp/pipe", os.O_RDONLY|syscall.O_NONBLOCK, 0600)
		if err != nil {
			fmt.Println(err)
		}
		var buff bytes.Buffer
		io.Copy(&buff, pipe)
		str, _ := buff.ReadString(0)
		amqpSender <- connector.AMQP10Message{Address: "lokean/logs", Body: str}
		pipe.Close()
	}
}
