package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
)

func chose(servers []string) string {
	return servers[rand.Intn(len(servers))]
}

// RestSender sends points using http protocol
type restSender struct {
	id      uint64
	servers []string
	port    uint16
}

func (sender *restSender) Send(data DataList) error {
	hostname := chose(sender.servers)
	url := fmt.Sprintf("http://%s:%d/api/put", hostname, sender.port)
	content, err := json.Marshal(data)
	if err != nil {
		return err
	}

	start := time.Now()
	response, err := http.Post(url, "application/json", bytes.NewReader(content))
	end := time.Now()
	Logger.WithFields(debugFields("Send", "restSender", "main")).Debugf("Start time: %s", start.Format(time.RFC3339))
	Logger.WithFields(debugFields("Send", "restSender", "main")).Debugf("End time: %s", end.Format(time.RFC3339))
	if err != nil {
		Logger.WithFields(debugFields("Send", "restSender", "main")).Debugf("Error: %s", err.Error())
		return err
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNoContent {
		Logger.WithFields(logrus.Fields{
			"gr":      sender.id,
			"method":  "http",
			"elapsed": end.Sub(start),
			"service": data[0].Tags["service"],
			"host":    data[0].Tags["host"],
		}).Infoln("Request executed")
	} else {
		io.Copy(os.Stderr, response.Body)
		Logger.WithFields(logrus.Fields{
			"gr":      sender.id,
			"method":  "http",
			"status":  response.StatusCode,
			"service": data[0].Tags["service"],
			"host":    data[0].Tags["host"],
		}).Errorln("Request failed")
	}
	return nil
}

// UDPSender sends points using UDP protocol
type udpSender struct {
	id      uint64
	servers []string
	port    uint16
}

func (sender *udpSender) Send(data DataList) error {
	for _, point := range data {
		hostname := chose(sender.servers)
		server := fmt.Sprintf("%s:%d", hostname, sender.port)
		content, err := json.Marshal(point)
		if err != nil {
			return err
		}

		start := time.Now()
		conn, err := net.Dial("udp", server)
		if err != nil {
			Logger.WithFields(debugFields("Send", "udpSender", "main")).Debugf("Error: %s", err.Error())
			return err
		}
		defer conn.Close()
		if _, err := conn.Write(content); err != nil {
			Logger.WithFields(debugFields("Send", "udpSender", "main")).Debugf("Error: %s", err.Error())
			return err
		}
		end := time.Now()
		Logger.WithFields(debugFields("Send", "udpSender", "main")).Debugf("Start time: %s", start.Format(time.RFC3339))
		Logger.WithFields(debugFields("Send", "udpSender", "main")).Debugf("End time: %s", end.Format(time.RFC3339))

		Logger.WithFields(logrus.Fields{
			"gr":      sender.id,
			"server":  hostname,
			"method":  "udp",
			"elapsed": end.Sub(start),
		}).Infoln("Request executed")
	}
	return nil
}
