package timeplus

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/timeplus/http"
	"github.com/redpanda-data/connect/v4/internal/impl/timeplus/proton"
)

const (
	protocolTimeplus  string = "timeplus"
	protocolTimeplusd string = "timeplusd"

	defaultTimeplusPort  = 80
	defaultTimeplusdPort = 8463
)

var outputConfigSpec *service.ConfigSpec

func init() {
	outputConfigSpec = service.NewConfigSpec()
	outputConfigSpec.
		Field(service.NewStringEnumField("protocol", protocolTimeplus, protocolTimeplusd).Default(protocolTimeplus)).
		Field(service.NewURLField("url").Examples("https://us.timeplus.cloud", "localhost")).
		Field(service.NewIntField("port").Optional().Description(fmt.Sprintf("[timeplus]: %d, [timeplusd]: %d", defaultTimeplusPort, defaultTimeplusdPort))).
		Field(service.NewStringField("workspace").Optional().Description("ID of the workspace. Required if protocol is `timeplus`")).
		Field(service.NewStringField("stream").Description("name of the stream")).
		Field(service.NewStringField("apikey").Default("").Optional().Description("[timeplus] the API key")).
		Field(service.NewBatchPolicyField("batching"))

	if err := service.RegisterBatchOutput("timeplus", outputConfigSpec, newTimeplusOutput); err != nil {
		panic(err)
	}
}

type timeplus struct {
	protocol string
	stream   string
	client   Writer
}

// Close implements service.Output
func (t *timeplus) Close(ctx context.Context) error {
	if t.client == nil {
		return nil
	}

	t.client = nil

	return nil
}

// Connect implements service.Output
func (t *timeplus) Connect(context.Context) error {
	if t.client == nil {
		return errors.New("client not initialized")
	}

	return nil
}

// TODO: Handle AsBytes
func (t *timeplus) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	if len(b) == 0 {
		return nil
	}

	cols := []string{}
	rows := [][]any{}
	for _, msg := range b {
		msgStructure, err := msg.AsStructured()
		if err != nil {
			continue
		}

		msgJSON, OK := msgStructure.(map[string]any)
		if !OK {
			continue
		}

		keys := []string{}
		data := []any{}
		for key := range msgJSON {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			data = append(data, msgJSON[key])
		}

		rows = append(rows, data)

		// Here we assume all messages have the same keys
		cols = keys
	}

	return t.client.Write(t.stream, cols, rows)
}

func newTimeplusOutput(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
	maxInFlight = 1

	baseURL, err := conf.FieldURL("url")
	if err != nil {
		return
	}

	protocol, err := conf.FieldString("protocol")
	if err != nil {
		return
	}

	stream, err := conf.FieldString("stream")
	if err != nil {
		return
	}

	apikey, err := conf.FieldString("apikey")
	if err != nil {
		return
	}

	var port int
	var workspace string
	if conf.Contains("port") {
		port, err = conf.FieldInt("port")
		if err != nil {
			return
		}
	}

	if protocol == protocolTimeplus {
		port = defaultTimeplusPort
		workspace, err = conf.FieldString("workspace")
		if err != nil {
			return
		}
		if len(workspace) == 0 {
			err = errors.New("workspace is required for `timeplus` protocol")
			return
		}
	} else if protocol == protocolTimeplusd {
		port = defaultTimeplusdPort
	}

	if conf.Contains("port") {
		port, err = conf.FieldInt("port")
		if err != nil {
			return
		}
	}

	if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
		return
	}

	logger := mgr.Logger()
	var client Writer

	if protocol == protocolTimeplus {
		client = http.NewClient(logger, baseURL, workspace, stream, apikey)
	} else if protocol == protocolTimeplusd {
		config := proton.DriverConfig{
			Host:     baseURL.String(),
			Port:     port,
			User:     "default",
			Password: "",
		}
		client = proton.NewDriver(logger, &config)
	}

	out = &timeplus{
		protocol: protocol,
		stream:   stream,
		client:   client,
	}

	return
}
