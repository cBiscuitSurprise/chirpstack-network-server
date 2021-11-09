package zmq

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "backend_zmq_event_count",
		Help: "The number of received events by the zmq backend (per event type).",
	}, []string{"event"})

	cc = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "backend_zmq_command_count",
		Help: "The number of published commands by the zmq backend (per command).",
	}, []string{"command"})
)

func zmqEventCounter(e string) prometheus.Counter {
	return ec.With(prometheus.Labels{"event": e})
}

func zmqCommandCounter(c string) prometheus.Counter {
	return cc.With(prometheus.Labels{"command": c})
}
