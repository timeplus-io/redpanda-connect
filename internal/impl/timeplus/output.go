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
	// TODO: add Version
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
		Field(service.NewStringField("apikey").Secret().Optional().Description("the API key. Required if you are sending message to Timeplus Enterprise Cloud")).
		Field(service.NewStringField("username").Optional().Description("the username")).
		Field(service.NewStringField("password").Secret().Optional().Description("the password")).
		Field(service.NewOutputMaxInFlightField()).
		Field(service.NewBatchPolicyField("batching"))

	if err := service.RegisterBatchOutput("timeplus", outputConfigSpec, newTimeplusOutput); err != nil {
		panic(err)
	}
}

type timeplus struct {
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

	// Here we assume all messages have the same structure, same keys
	for _, msg := range b {
		keys := []string{}
		data := []any{}

		if msgStructure, err := msg.AsStructured(); err != nil {
			// As bytes
			bytes, err := msg.AsBytes()
			if err != nil {
				continue
			}

			keys = append(keys, "raw")
			data = append(data, string(bytes))
		} else {
			// As structured
			msgJSON, OK := msgStructure.(map[string]any)
			if !OK {
				continue
			}

			for key := range msgJSON {
				keys = append(keys, key)
			}
			sort.Strings(keys)

			for _, key := range keys {
				data = append(data, msgJSON[key])
			}
		}

		rows = append(rows, data)
		cols = keys
	}

	return t.client.Write(ctx, cols, rows)
}

func newTimeplusOutput(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
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
	if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
		return
	}

	logger := mgr.Logger()
	var client Writer

	client = http.NewClient(logger, maxInFlight, target, baseURL, workspace, stream, apikey, username, password)

	out = &timeplus{
		client: client,
	}

	return
}
