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
)

var inputConfigSpec *service.ConfigSpec

func init() {
	inputConfigSpec = service.NewConfigSpec().
		Summary("Reads messages from Timeplus Enterprise.").
		Field(service.NewStringField("query")).
		Field(service.NewURLField("url").Default("tcp://localhost:8463")).
		Field(service.NewStringField("username")).
		Field(service.NewStringField("password").Secret())

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

	username, err := conf.FieldString("username")
	if err != nil {
		return nil, err
	}

	password, err := conf.FieldString("password")
	if err != nil {
		return nil, err
	}

	driver := driver.NewDriver(logger, addr.Host, username, password)

	return service.AutoRetryNacks(
		&timeplusInput{
			log:    logger,
			reader: driver,
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
