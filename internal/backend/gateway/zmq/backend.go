package zmq

import (
	"context"
	"encoding/base64"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/brocaar/chirpstack-api/go/v3/gw"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/gateway"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/gateway/marshaler"
	"github.com/brocaar/chirpstack-network-server/v3/internal/config"
	"github.com/brocaar/chirpstack-network-server/v3/internal/helpers"
	"github.com/brocaar/chirpstack-network-server/v3/internal/storage"
	"github.com/brocaar/lorawan"
)

const deduplicationLockTTL = time.Millisecond * 500
const (
	marshalerV2JSON = iota
	marshalerProtobuf
	marshalerJSON
)

// Backend implements a ZMQ backend.
type Backend struct {
	sync.RWMutex

	wg sync.WaitGroup

	eventSockCancel   func()
	commandSockCancel func()
	eventSock         zmq4.Socket
	commandSock       zmq4.Socket
	commandMux        sync.Mutex

	rxPacketChan      chan gw.UplinkFrame
	statsPacketChan   chan gw.GatewayStats
	downlinkTXAckChan chan gw.DownlinkTXAck

	eventURL   string
	commandURL string

	downMode         string
	gatewayMarshaler map[lorawan.EUI64]marshaler.Type
}

// NewBackend creates a new Backend.
func NewBackend(conf config.Config) (*Backend, error) {
	zmqConf := conf.NetworkServer.Gateway.Backend.ZMQ

	log.WithFields(log.Fields{
		"event_url":   zmqConf.EventURL,
		"command_url": zmqConf.CommandURL,
	}).Info("integration/zmq: setting up integration")

	b := Backend{
		rxPacketChan:      make(chan gw.UplinkFrame),
		statsPacketChan:   make(chan gw.GatewayStats),
		downlinkTXAckChan: make(chan gw.DownlinkTXAck),
		gatewayMarshaler:  make(map[lorawan.EUI64]marshaler.Type),
		eventURL:          zmqConf.EventURL,
		commandURL:        zmqConf.CommandURL,
		downMode:          conf.NetworkServer.Gateway.Backend.MultiDownlinkFeature,
	}

	// go b.Open()

	return &b, nil
}

// Open opens the backend.
func (b *Backend) Open() error {
	log.WithField("event-socket", b.eventURL).Info("gateway/zmq: connecting to event socket")
	b.dialEventSockLoop()

	log.WithField("command-socket", b.commandURL).Info("gateway/zmq: connecting to commmand socket")
	b.dialCommandSockLoop()

	go b.eventLoop()

	return nil
}

// Close closes the backend.
func (b *Backend) Close() error {
	log.Info("gateway/zmq: closing backend")

	log.WithField("event-socket", b.eventURL).Info("gateway/zmq: disconnecting from event socket")
	log.WithField("command-socket", b.commandURL).Info("gateway/zmq: disconnecting from commmand socket")
	b.eventSock.Close()
	b.commandSock.Close()

	b.eventSockCancel()
	b.commandSockCancel()

	return nil
}

func (b *Backend) dialEventSock() error {
	ctx := context.Background()
	ctx, b.eventSockCancel = context.WithCancel(ctx)

	b.eventSock = zmq4.NewSub(ctx)
	err := b.eventSock.Dial(b.eventURL)
	if err != nil {
		return errors.Wrap(err, "dial event api url error")
	}

	err = b.eventSock.SetOption(zmq4.OptionSubscribe, "")
	if err != nil {
		return errors.Wrap(err, "set event option error")
	}

	log.WithFields(log.Fields{
		"event_url": b.eventURL,
	}).Info("integration/zmq: connected to event socket")

	return nil
}

func (b *Backend) dialCommandSock() error {
	ctx := context.Background()
	ctx, b.commandSockCancel = context.WithCancel(ctx)

	b.commandSock = zmq4.NewReq(ctx)
	err := b.commandSock.Dial(b.commandURL)
	if err != nil {
		return errors.Wrap(err, "dial command api url error")
	}

	log.WithFields(log.Fields{
		"command_url": b.commandURL,
	}).Info("integration/zmq: connected to command socket")

	return nil
}

func (b *Backend) dialCommandSockLoop() {
	for {
		if err := b.dialCommandSock(); err != nil {
			log.WithError(err).Error("integration/zmq: command socket dial error")
			time.Sleep(time.Second)
			continue
		}
		break
	}
}

func (b *Backend) dialEventSockLoop() {
	for {
		if err := b.dialEventSock(); err != nil {
			log.WithError(err).Error("integration/zmq: event socket dial error")
			time.Sleep(time.Second)
			continue
		}
		break
	}
}

// RXPacketChan returns the uplink-frame channel.
func (b *Backend) RXPacketChan() chan gw.UplinkFrame {
	return b.rxPacketChan
}

// StatsPacketChan returns the gateway stats channel.
func (b *Backend) StatsPacketChan() chan gw.GatewayStats {
	return b.statsPacketChan
}

// DownlinkTXAckChan returns the downlink tx ack channel.
func (b *Backend) DownlinkTXAckChan() chan gw.DownlinkTXAck {
	return b.downlinkTXAckChan
}

// SendTXPacket sends the given downlink-frame to the gateway.
func (b *Backend) SendTXPacket(txPacket gw.DownlinkFrame) error {
	gatewayID := helpers.GetGatewayID(&txPacket)
	downID := helpers.GetDownlinkID(&txPacket)

	if err := gateway.UpdateDownlinkFrame(b.downMode, &txPacket); err != nil {
		return errors.Wrap(err, "set downlink compatibility mode error")
	}

	return b.publishCommand(log.Fields{
		"downlink_id": downID,
	}, gatewayID, "down", &txPacket)
}

// SendGatewayConfigPacket sends the given GatewayConfigPacket to the gateway.
func (b *Backend) SendGatewayConfigPacket(configPacket gw.GatewayConfiguration) error {
	gatewayID := helpers.GetGatewayID(&configPacket)

	return b.publishCommand(log.Fields{}, gatewayID, "config", &configPacket)
}

func (b *Backend) publishCommand(fields log.Fields, gatewayID lorawan.EUI64, command string, msg proto.Message) error {
	b.commandMux.Lock()
	defer b.commandMux.Unlock()

	var err error
	t := b.getGatewayMarshaler(gatewayID)
	bb, err := marshaler.MarshalCommand(t, msg)
	if err != nil {
		return errors.Wrap(err, "gateway/zmq: marshal gateway command error")
	}

	fields["gateway_id"] = gatewayID
	fields["command"] = command
	log.WithFields(fields).Info("gateway/zmq: publishing gateway command")

	zmqCommandCounter(command).Inc()

	// in testing, it seems like zmq4-0.13.0 doesn't append a nil byte for
	// multipart while zmq4-0.7.0 did ... copied the line from zmq4@v0.7.0/rep.go:40
	payload := zmq4.NewMsgFrom([]byte{0}, []byte(command), bb)
	if err = b.commandSock.SendMulti(payload); err != nil {
		b.commandSockCancel()
		b.dialCommandSock()
		return errors.Wrap(err, "gateway/zmq: send command request error")
	}

	reply, err := b.commandSock.Recv()
	if err != nil {
		b.commandSockCancel()
		b.dialCommandSock()
		return errors.Wrap(err, "gateway/zmq: receive command request reply error")
	}

	log.WithFields(fields).Info("gateway/zmq: " + string(reply.Bytes()))

	// frames := []string{command + " " + string(bb)}
	// err = b.commandSock.Send(zmq4.NewMsgFromString(frames))
	// if err != nil {
	// 	return errors.Wrap(err, "gateway/zmq: publish gateway command error")
	// }

	return nil
}

func (b *Backend) rxPacketHandler(payload []byte) {
	b.wg.Add(1)
	defer b.wg.Done()

	var uplinkFrame gw.UplinkFrame
	t, err := marshaler.UnmarshalUplinkFrame(payload, &uplinkFrame)
	if err != nil {
		log.WithFields(log.Fields{
			"data_base64": base64.StdEncoding.EncodeToString(payload),
		}).WithError(err).Error("gateway/zmq: unmarshal uplink frame error")
		return
	}

	if uplinkFrame.TxInfo == nil {
		log.WithFields(log.Fields{
			"data_base64": base64.StdEncoding.EncodeToString(payload),
		}).Error("gateway/zmq: tx_info must not be nil")
		return
	}

	if uplinkFrame.RxInfo == nil {
		log.WithFields(log.Fields{
			"data_base64": base64.StdEncoding.EncodeToString(payload),
		}).Error("gateway/zmq: rx_info must not be nil")
		return
	}

	gatewayID := helpers.GetGatewayID(uplinkFrame.RxInfo)
	b.setGatewayMarshaler(gatewayID, t)
	uplinkID := helpers.GetUplinkID(uplinkFrame.RxInfo)

	log.WithFields(log.Fields{
		"uplink_id":  uplinkID,
		"gateway_id": gatewayID,
	}).Info("gateway/zmq: uplink frame received")

	b.rxPacketChan <- uplinkFrame
}

func (b *Backend) statsPacketHandler(payload []byte) {
	b.wg.Add(1)
	defer b.wg.Done()

	var gatewayStats gw.GatewayStats
	t, err := marshaler.UnmarshalGatewayStats(payload, &gatewayStats)
	if err != nil {
		log.WithFields(log.Fields{
			"data_base64": base64.StdEncoding.EncodeToString(payload),
		}).WithError(err).Error("gateway/mqtt: unmarshal gateway stats error")
		return
	}

	gatewayID := helpers.GetGatewayID(&gatewayStats)
	statsID := helpers.GetStatsID(&gatewayStats)
	b.setGatewayMarshaler(gatewayID, t)

	key := storage.GetRedisKey("lora:ns:stats:lock:%s:%s", gatewayID, statsID)
	if locked, err := b.isLocked(key); err != nil || locked {
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"key":      key,
				"stats_id": statsID,
			}).Error("gateway/mqtt: acquire lock error")
		}

		return
	}

	log.WithFields(log.Fields{
		"gateway_id": gatewayID,
		"stats_id":   statsID,
	}).Info("gateway/mqtt: gateway stats packet received")
	b.statsPacketChan <- gatewayStats
}

func (b *Backend) ackPacketHandler(payload []byte) {
	b.wg.Add(1)
	defer b.wg.Done()

	var ack gw.DownlinkTXAck
	t, err := marshaler.UnmarshalDownlinkTXAck(payload, &ack)
	if err != nil {
		log.WithFields(log.Fields{
			"data_base64": base64.StdEncoding.EncodeToString(payload),
		}).WithError(err).Error("backend/gateway: unmarshal downlink tx ack error")
	}

	gatewayID := helpers.GetGatewayID(&ack)
	downlinkID := helpers.GetDownlinkID(&ack)
	b.setGatewayMarshaler(gatewayID, t)

	key := storage.GetRedisKey("lora:ns:ack:lock:%s:%s", gatewayID, downlinkID)
	if locked, err := b.isLocked(key); err != nil || locked {
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"key":         key,
				"downlink_id": downlinkID,
			}).Error("gateway/mqtt: acquire lock error")
		}

		return
	}

	log.WithFields(log.Fields{
		"gateway_id":  gatewayID,
		"downlink_id": downlinkID,
	}).Info("backend/gateway: downlink tx acknowledgement received")
	b.downlinkTXAckChan <- ack
}

func (b *Backend) eventLoop() {
	for {
		msg, err := b.eventSock.Recv()
		if err != nil {
			log.WithError(err).Error("integration/zmq: receive event message error")

			// We need to recover both the event and command sockets.
			func() {
				b.commandMux.Lock()
				defer b.commandMux.Unlock()

				b.eventSockCancel()
				b.commandSockCancel()
				b.dialEventSockLoop()
				b.dialCommandSockLoop()
			}()
			continue
		}

		if len(msg.Frames) == 0 {
			continue
		}

		if len(msg.Frames) != 2 {
			log.WithFields(log.Fields{
				"frame_count": len(msg.Frames),
			}).Error("integration/zmq: expected 2 frames in event message")
			continue
		}

		switch string(msg.Frames[0]) {
		case "up":
			zmqEventCounter("up").Inc()
			b.rxPacketHandler(msg.Frames[1])
		case "ack":
			zmqEventCounter("ack").Inc()
			b.ackPacketHandler(msg.Frames[1])
		case "stats":
			zmqEventCounter("stats").Inc()
			b.statsPacketHandler(msg.Frames[1])
		default:
			log.WithFields(log.Fields{
				"event": string(msg.Frames[0]),
			}).Error("integration/zmq: unexpected event received")
			continue
		}

		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"event": string(msg.Frames[0]),
			}).Error("integration/zmq: handle event error")
		}
	}
}

func (b *Backend) setGatewayMarshaler(gatewayID lorawan.EUI64, t marshaler.Type) {
	b.Lock()
	defer b.Unlock()

	b.gatewayMarshaler[gatewayID] = t
}

func (b *Backend) getGatewayMarshaler(gatewayID lorawan.EUI64) marshaler.Type {
	b.RLock()
	defer b.RUnlock()

	return b.gatewayMarshaler[gatewayID]
}

// isLocked returns if a lock exists for the given key, if false a lock is
// acquired.
func (b *Backend) isLocked(key string) (bool, error) {
	set, err := storage.RedisClient().SetNX(context.Background(), key, "lock", deduplicationLockTTL).Result()
	if err != nil {
		return false, errors.Wrap(err, "acquire lock error")
	}

	// Set is true when we were able to set the lock, we return true if it
	// was already locked.
	return !set, nil
}
