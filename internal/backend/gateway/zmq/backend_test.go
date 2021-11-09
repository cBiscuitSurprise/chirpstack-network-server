package zmq

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/go-zeromq/zmq4"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/brocaar/chirpstack-api/go/v3/gw"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/gateway"
	"github.com/brocaar/chirpstack-network-server/v3/internal/storage"
	"github.com/brocaar/chirpstack-network-server/v3/internal/test"
)

type BackendTestSuite struct {
	suite.Suite

	backend gateway.Gateway

	pubSock zmq4.Socket
	repSock zmq4.Socket
}

func (ts *BackendTestSuite) SetupSuite() {
	assert := require.New(ts.T())

	conf := test.GetConfig()
	assert.NoError(storage.Setup(conf))
}

func (ts *BackendTestSuite) SetupTest() {
	storage.RedisClient().FlushAll(context.Background())

	assert := require.New(ts.T())

	tempDir, err := ioutil.TempDir("", "test")
	assert.NoError(err)

	ts.pubSock = zmq4.NewPub(context.Background())
	ts.repSock = zmq4.NewRep(context.Background())

	assert.NoError(ts.pubSock.Listen(fmt.Sprintf("ipc://%s/events", tempDir)))
	assert.NoError(ts.repSock.Listen(fmt.Sprintf("ipc://%s/commands", tempDir)))

	conf := test.GetConfig()
	conf.NetworkServer.Gateway.Backend.ZMQ.EventURL = fmt.Sprintf("ipc://%s/events", tempDir)
	conf.NetworkServer.Gateway.Backend.ZMQ.CommandURL = fmt.Sprintf("ipc://%s/commands", tempDir)

	ts.backend, err = NewBackend(conf)
	assert.NoError(err)

	assert.NoError(ts.backend.Open())
}

func (ts *BackendTestSuite) TearDownTest() {
	assert := require.New(ts.T())
	assert.NoError(ts.backend.Close())
}

func (ts *BackendTestSuite) TestUplinkFrame() {
	assert := require.New(ts.T())

	uplinkFrame := gw.UplinkFrame{
		PhyPayload: []byte{1, 2, 3, 4},
		TxInfo: &gw.UplinkTXInfo{
			Frequency: 868100000,
		},
		RxInfo: &gw.UplinkRXInfo{
			GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
	}

	b, err := proto.Marshal(&uplinkFrame)
	assert.NoError(err)
	uplinkFrame.XXX_sizecache = 0
	uplinkFrame.TxInfo.XXX_sizecache = 0
	uplinkFrame.RxInfo.XXX_sizecache = 0

	assert.NoError(ts.pubSock.SendMulti(zmq4.Msg{
		Frames: [][]byte{
			[]byte("up"),
			b,
		},
	}))

	receivedUplink := <-ts.backend.RXPacketChan()
	assert.EqualValues(uplinkFrame, receivedUplink)
}

func (ts *BackendTestSuite) TestGatewayStats() {
	assert := require.New(ts.T())

	gatewayStats := gw.GatewayStats{
		GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}

	b, err := proto.Marshal(&gatewayStats)
	assert.NoError(err)
	gatewayStats.XXX_sizecache = 0

	assert.NoError(ts.pubSock.SendMulti(zmq4.Msg{
		Frames: [][]byte{
			[]byte("stats"),
			b,
		},
	}))

	receivedStats := <-ts.backend.StatsPacketChan()
	assert.EqualValues(gatewayStats, receivedStats)
}

// func (ts *BackendTestSuite) TestDownlinkTXAck() {
// 	assert := require.New(ts.T())

// 	downlinkTXAck := gw.DownlinkTXAck{
// 		GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
// 	}
// 	b, err := proto.Marshal(&downlinkTXAck)
// 	assert.NoError(err)

// 	assert.NoError(ts.pubSock.SendMulti(zmq4.Msg{
// 		Frames: [][]byte{
// 			[]byte("ack"),
// 			b,
// 		},
// 	}))

// 	receivedAck := <-ts.backend.DownlinkTXAckChan()
// 	if !proto.Equal(&downlinkTXAck, &receivedAck) {
// 		assert.Equal(downlinkTXAck, receivedAck)
// 	}
// }

func (ts *BackendTestSuite) TestSendDownlinkFrame() {
	assert := require.New(ts.T())

	ack := gw.DownlinkTXAck{
		GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	ackB, err := proto.Marshal(&ack)
	assert.NoError(err)

	ts.T().Run("hybrid", func(t *testing.T) {
		downlinkFrameChanHybrid := make(chan gw.DownlinkFrame)
		go ts.waitForDownlinkRequest("down", downlinkFrameChanHybrid, ackB)

		assert := require.New(t)
		ts.backend.(*Backend).downMode = "hybrid"

		assert.NoError(ts.backend.SendTXPacket(gw.DownlinkFrame{
			GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Items: []*gw.DownlinkFrameItem{
				{
					PhyPayload: []byte{1, 2, 3},
					TxInfo:     &gw.DownlinkTXInfo{},
				},
			},
		}))

		downlink := <-downlinkFrameChanHybrid
		proto.Equal(&downlink, &gw.DownlinkFrame{
			GatewayId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
			PhyPayload: []byte{1, 2, 3},
			TxInfo: &gw.DownlinkTXInfo{
				GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			},
			Items: []*gw.DownlinkFrameItem{
				{
					PhyPayload: []byte{1, 2, 3},
				},
			},
		})
	})

	ts.T().Run("multi_only", func(t *testing.T) {
		downlinkFrameChanMultiOnly := make(chan gw.DownlinkFrame)
		go ts.waitForDownlinkRequest("down", downlinkFrameChanMultiOnly, ackB)

		assert := require.New(t)
		ts.backend.(*Backend).downMode = "multi_only"

		assert.NoError(ts.backend.SendTXPacket(gw.DownlinkFrame{
			GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Items: []*gw.DownlinkFrameItem{
				{
					PhyPayload: []byte{1, 2, 3},
					TxInfo:     &gw.DownlinkTXInfo{},
				},
			},
		}))

		downlink := <-downlinkFrameChanMultiOnly
		proto.Equal(&downlink, &gw.DownlinkFrame{
			GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Items: []*gw.DownlinkFrameItem{
				{
					PhyPayload: []byte{1, 2, 3},
				},
			},
		})
	})

	ts.T().Run("legacy", func(t *testing.T) {
		downlinkFrameChanLegacy := make(chan gw.DownlinkFrame)
		go ts.waitForDownlinkRequest("down", downlinkFrameChanLegacy, ackB)

		assert := require.New(t)
		ts.backend.(*Backend).downMode = "legacy"

		assert.NoError(ts.backend.SendTXPacket(gw.DownlinkFrame{
			GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Items: []*gw.DownlinkFrameItem{
				{
					PhyPayload: []byte{1, 2, 3},
					TxInfo:     &gw.DownlinkTXInfo{},
				},
			},
		}))

		downlink := <-downlinkFrameChanLegacy
		proto.Equal(&downlink, &gw.DownlinkFrame{
			PhyPayload: []byte{1, 2, 3},
			TxInfo: &gw.DownlinkTXInfo{
				GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			},
		})
	})
}

func (ts *BackendTestSuite) TestSendGatewayConfiguration() {
	assert := require.New(ts.T())

	gatewayConfigChan := make(chan gw.GatewayConfiguration)
	go ts.waitForConfigRequest("config", gatewayConfigChan, []byte{0})

	gatewayConfig := gw.GatewayConfiguration{
		GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Version:   "1.2.3",
	}
	assert.NoError(ts.backend.SendGatewayConfigPacket(gatewayConfig))
	gatewayConfig.XXX_sizecache = 0

	configReceived := <-gatewayConfigChan
	assert.Equal(gatewayConfig, configReceived)
}

func (ts *BackendTestSuite) waitForDownlinkRequest(expectedCommand string, channel chan<- gw.DownlinkFrame, reply []byte) {
	assert := require.New(ts.T())
	msg, err := ts.repSock.Recv()
	assert.NoError(err)

	assert.EqualValues(expectedCommand, string(msg.Frames[0]))

	var pl gw.DownlinkFrame
	if err := proto.Unmarshal(msg.Frames[1], &pl); err != nil {
		panic(err)
	}

	assert.NoError(ts.repSock.Send(zmq4.NewMsg(reply)))
	channel <- pl
}

func (ts *BackendTestSuite) waitForConfigRequest(expectedCommand string, channel chan<- gw.GatewayConfiguration, reply []byte) {
	assert := require.New(ts.T())
	msg, err := ts.repSock.Recv()
	assert.NoError(err)

	assert.EqualValues(expectedCommand, string(msg.Frames[0]))

	var pl gw.GatewayConfiguration
	if err := proto.Unmarshal(msg.Frames[1], &pl); err != nil {
		panic(err)
	}

	assert.NoError(ts.repSock.Send(zmq4.NewMsg(reply)))
	channel <- pl
}

func TestBackend(t *testing.T) {
	suite.Run(t, new(BackendTestSuite))
}
