package timeplus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/timeplus/driver"
	"github.com/redpanda-data/connect/v4/internal/impl/timeplus/http"
)

var inputConfigSpec *service.ConfigSpec

func init() {
	inputConfigSpec = service.NewConfigSpec().
		Categories("Services").
		Summary("Executes a query on Timeplus Enterprise and creates a message from each row received").
		Description(`
This input can execute a query on Timeplus Enterprise Cloud, Timeplus Enterprise (self-hosted) or Timeplusd. A structured message will be created
from each row received.

If it is a streaming query, this input will keep running until the query is terminated. If it is a table query, this input will shuts down once the rows from the query are exhausted.`).
		Example(
			"From Timeplus Enterprise Cloud",
			"You will need to create API Key on Timeplus Enterprise Cloud Web console first and then set the `apikey` field.",
			`
input:
  timeplus:
    url: https://us-west-2.timeplus.cloud
    workspace: my_workspace_id
    query: select * from iot
    apikey: <Your API Key>`).
		Example(
			"From Timeplus Enterprise (self-hosted)",
			"For self-housted Timeplus Enterprise, you will need to specify the username and password as well as the URL of the App server",
			`
output:
  timeplus:
    url: http://localhost:8000
    workspace: my_workspace_id
    query: select * from iot
    username: username
    password: pw`).
		Example(
			"From Timeplusd",
			"Make sure the the schema of url is tcp",
			`
input:
  timeplus:
    url: tcp://localhost:8463
    query: select * from iot
    username: timeplus
    password: timeplus`)

	inputConfigSpec.
		Field(service.NewStringField("query").Description("The query to run").Examples("select * from iot", "select count(*) from table(iot)")).
		Field(service.NewURLField("url").Description("The url should always include schema and host.").Default("tcp://localhost:8463")).
		Field(service.NewStringField("workspace").Optional().Description("ID of the workspace. Required when reads from Timeplus Enterprise.")).
		Field(service.NewStringField("apikey").Secret().Optional().Description("The API key. Required when reads from Timeplus Enterprise Cloud")).
		Field(service.NewStringField("username").Optional().Description("The username. Required when reads from Timeplus Enterprise (self-hosted) or Timeplusd")).
		Field(service.NewStringField("password").Secret().Optional().Description("The password. Required when reads from Timeplus Enterprise (self-hosted) or Timeplusd"))
	err := service.RegisterInput(
		"timeplus", inputConfigSpec, newTimeplusInput)
	if err != nil {
		panic(err)
	}
}

func newTimeplusInput(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	logger := mgr.Logger()
	sql, err := conf.FieldString("query")
	if err != nil {
		return nil, err
	}

	addr, err := conf.FieldURL("url")
	if err != nil {
		return nil, err
	}

	var (
		apikey   string
		username string
		password string
	)
	if conf.Contains("apikey") {
		apikey, err = conf.FieldString("apikey")
		if err != nil {
			return nil, err
		}
	}
	if conf.Contains("username") {
		username, err = conf.FieldString("username")
		if err != nil {
			return nil, err
		}
	}
	if conf.Contains("password") {
		password, err = conf.FieldString("password")
		if err != nil {
			return nil, err
		}
	}

	var reader Reader

	if addr.Scheme == "tcp" {
		reader = driver.NewDriver(logger, addr.Host, username, password)
	} else {
		workspace, err := conf.FieldString("workspace")
		if err != nil {
			return nil, err
		}

		reader = http.NewSSEClient(logger, addr, workspace, apikey, username, password)
	}

	return service.AutoRetryNacks(
		&timeplusInput{
			log:    logger,
			reader: reader,
			sql:    sql,
		}), nil
}

type timeplusInput struct {
	log *service.Logger

	reader Reader
	sql    string

	stopQuery context.CancelFunc
}

func (p *timeplusInput) Connect(ctx context.Context) error {
	logger := p.log.With("sql", p.sql)
	queryCtx, stopQuery := context.WithCancel(context.Background())
	p.stopQuery = stopQuery

	if err := p.reader.Run(queryCtx, p.sql); err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return fmt.Errorf("failed to connect to driver")
		} else if errors.Is(err, os.ErrDeadlineExceeded) {
			return fmt.Errorf("failed to connect to driver")
		}

		return fmt.Errorf("failed to run query: %w", err)
	}

	logger.Info("timeplusd connected")

	return nil
}

func (p *timeplusInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	event, err := p.reader.Read(ctx)
	if err != nil {
		// timeplusd input always read from a MV which is supposed will never end,
		// Thus, if it encounters an EOF error, that means the connection is terminated for some reason, for example,
		// during timeplusd upgrade, the pod is terminated for a new one. So here, we return ErrNotConnected so that
		// bethos will retry reconnect to timeplusd.
		if errors.Is(err, io.EOF) {
			return nil, nil, service.ErrNotConnected
		}

		// TODO check if it's possible to identify if it's a connection error or not, if it's a connection error,
		//      it should return `service.ErrNotConnected` so that benthos will handle reconnection
		return nil, nil, err
	}

	msg := service.NewMessage(nil)
	msg.SetStructured(event)

	ack := func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}

	return msg, ack, nil
}

func (p *timeplusInput) Close(ctx context.Context) error {
	p.stopQuery()
	return p.reader.Close()
}
