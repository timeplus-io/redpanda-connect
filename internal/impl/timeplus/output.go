package timeplus

import (
	"context"
	"errors"
	"sort"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/timeplus/http"
)

const (
	targetTimeplus  string = "timeplus"
	targetTimeplusd string = "timeplusd"
)

var outputConfigSpec *service.ConfigSpec

func init() {
	outputConfigSpec = service.NewConfigSpec().
		Categories("Services").
		Summary("Sends message to a Timeplus Enterprise stream").
		Description(`
This output can send message to Timeplus Enterprise Cloud, Timeplus Enterprise Onprem or directly to timeplusd.

A sample config to send the data to Timeplus Enterprise Cloud
` + "```yml" + `
workspace: my_workspace_id
stream: mystream
apikey: fdsjklajfkldsajkl
` + "```" + `

A sample config to send the data to Timeplus Enterprise Onprem
` + "```yml" + `
url: http://localhost:8000
workspace: my_workspace_id
stream: mystream
username: timeplusd
password: timeplusd
` + "```" + `

A sample config to send the data to timeplusd
` + "```yml" + `
url: http://localhost:3218
stream: mystream
username: timeplusd
password: timeplusd
` + "```" + `
`)

	outputConfigSpec.
		Field(service.NewStringEnumField("target", targetTimeplus, targetTimeplusd).Default(targetTimeplus)).
		Field(service.NewURLField("url").Examples("https://us.timeplus.cloud", "localhost")).
		Field(service.NewStringField("workspace").Optional().Description("ID of the workspace. Required if target is `timeplus`.")).
		Field(service.NewStringField("stream").Description("name of the stream")).
		Field(service.NewStringField("apikey").Secret().Optional().Description("the API key")).
		Field(service.NewStringField("username").Optional().Description("the username")).
		Field(service.NewStringField("password").Secret().Optional().Description("the password")).
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

	var (
		apikey   string
		username string
		password string
	)
	if conf.Contains("apikey") {
		apikey, err = conf.FieldString("apikey")
		if err != nil {
			return
		}
	}
	if conf.Contains("username") {
		username, err = conf.FieldString("username")
		if err != nil {
			return
		}
	}
	if conf.Contains("password") {
		password, err = conf.FieldString("password")
		if err != nil {
			return
		}
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

	client = http.NewClient(logger, target, baseURL, workspace, stream, apikey, username, password)

	out = &timeplus{
		target: target,
		stream: stream,
		client: client,
	}

	return
}
