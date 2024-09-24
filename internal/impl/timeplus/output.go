package timeplus

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/timeplus/http"
)

const (
	targetTimeplus  string = "timeplus"
	targetTimeplusd string = "timeplusd"

	defaultTimeplusPort  = 80
	defaultTimeplusdPort = 8463
)

var outputConfigSpec *service.ConfigSpec

func init() {
	outputConfigSpec = service.NewConfigSpec()
	outputConfigSpec.
		Field(service.NewStringEnumField("target", targetTimeplus, targetTimeplusd).Default(targetTimeplus)).
		Field(service.NewURLField("url").Examples("https://us.timeplus.cloud", "localhost")).
		Field(service.NewIntField("port").Optional().Description(fmt.Sprintf("[timeplus]: %d, [timeplusd]: %d", defaultTimeplusPort, defaultTimeplusdPort))).
		Field(service.NewStringField("workspace").Optional().Description("ID of the workspace. Required if target is `timeplus`")).
		Field(service.NewStringField("stream").Description("name of the stream")).
		Field(service.NewStringField("apikey").Default("").Optional().Description("[timeplus] the API key")).
		Field(service.NewBatchPolicyField("batching"))

	if err := service.RegisterBatchOutput("timeplus", outputConfigSpec, newTimeplusOutput); err != nil {
		panic(err)
	}
}

type timeplus struct {
	target string
	stream string
	client Writer
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

	target, err := conf.FieldString("target")
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

	var workspace string

	if target == targetTimeplus {
		workspace, err = conf.FieldString("workspace")
		if err != nil {
			return
		}
		if len(workspace) == 0 {
			err = errors.New("workspace is required for `timeplus` target")
			return
		}
	}

	if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
		return
	}

	logger := mgr.Logger()
	var client Writer

	client = http.NewClient(logger, target, baseURL, workspace, stream, apikey)

	out = &timeplus{
		target: target,
		stream: stream,
		client: client,
	}

	return
}
