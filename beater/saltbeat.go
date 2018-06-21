package beater

import (
	"fmt"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"bytes"
	"net"
	"reflect"

	"github.com/martinhoefling/saltbeat/config"
	"github.com/ugorji/go/codec"
)

type Saltbeat struct {
	beatConfig       *config.Config
	done             chan struct{}
	messages         chan map[string]interface{}
	socketConnection *net.UnixConn
	client           publisher.Client
}

// Creates beater
func New() *Saltbeat {
	logp.Debug("beater", "Creating new beater")
	return &Saltbeat{
		done:     make(chan struct{}),
		messages: make(chan map[string]interface{}),
	}
}

/// *** Beater interface methods ***///

func (bt *Saltbeat) Config(b *beat.Beat) error {
	logp.Debug("beater", "Configuring beater")

	// Load beater beatConfig
	err := b.RawConfig.Unpack(&bt.beatConfig)
	if err != nil {
		return fmt.Errorf("Error reading config file: %v", err)
	}

	return nil
}

func (bt *Saltbeat) Setup(b *beat.Beat) error {
	logp.Debug("beater", "Setting up beater")
	// Setting default period if not set
	if bt.beatConfig.Saltbeat.MasterEventPub == "" {
		bt.beatConfig.Saltbeat.MasterEventPub = "/var/run/salt/master/master_event_pub.ipc"
	}
	bt.client = b.Publisher.Connect()

	var err error
	logp.Info("Opening socket %s", bt.beatConfig.Saltbeat.MasterEventPub)
	bt.socketConnection, err = net.DialUnix("unix", nil, &net.UnixAddr{bt.beatConfig.Saltbeat.MasterEventPub, "unix"})
	if err != nil {
		return err
	}
	err = bt.socketConnection.CloseWrite()
	if err != nil {
		return err
	}

	go func() {
		var err error
		var handle codec.MsgpackHandle
		handle.MapType = reflect.TypeOf(map[string]interface{}(nil))
		for {
			logp.Debug("message", "Waiting for message")
			message_decoder := codec.NewDecoder(bt.socketConnection, &handle)
			var message map[string]interface{}
			err = message_decoder.Decode(&message)
			if err != nil {
				logp.WTF(strings.Replace(err.Error(), "%", "%%", -1))
			}
			logp.Debug("message", "Message read")
			bt.messages <- message
		}
	}()

	return nil
}

func parseMessage(handle codec.MsgpackHandle, message map[string]interface{}) (string, map[string]interface{}) {
	body := message["body"].([]byte)
	newline := byte(10)
	splitted := bytes.SplitN(body, []byte{newline, newline}, 2)

	tag := string(splitted[0])
	logp.Debug("message", "Message tag is %s", tag)

	payload_bytes := splitted[1]
	payload_decoder := codec.NewDecoderBytes(payload_bytes, &handle)

	var payload map[string]interface{}
	err := payload_decoder.Decode(&payload)
	if err != nil {
		logp.WTF(err.Error())
	}

	logp.Debug("message", "Decoded payload is %s", payload)
	return tag, payload
}

func (bt *Saltbeat) Run(b *beat.Beat) error {
	logp.Info("saltbeat is running! Hit CTRL-C to stop it.")

	var err error
	var handle codec.MsgpackHandle
	handle.MapType = reflect.TypeOf(map[string]interface{}(nil))
	handle.RawToString = true

	for {
		select {
		case <-bt.done:
			return nil
		case message := <-bt.messages:
			tag, payload := parseMessage(handle, message)
			logp.Debug("publish", "Publishing event")

			event := common.MapStr{
				"@timestamp": common.Time(time.Now()),
				"type":       b.Name,
				"tag":        tag,
				"payload":    payload,
			}

			ok := bt.client.PublishEvent(event)
			if !ok {
				logp.Debug("publish", "Cannot publish event")
				logp.WTF(err.Error())
			}
			logp.Debug("publish", "Published")
		}
	}
}

func (bt *Saltbeat) Cleanup(b *beat.Beat) error {
	logp.Info("Closing socket %s", bt.beatConfig.Saltbeat.MasterEventPub)
	bt.socketConnection.Close()
	return nil
}

func (bt *Saltbeat) Stop() {
	close(bt.done)
	close(bt.messages)
}
