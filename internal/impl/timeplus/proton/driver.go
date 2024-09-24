package proton

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	protonDriver "github.com/timeplus-io/proton-go-driver/v2"
)

type Driver struct {
	logger *service.Logger
	conn   *sql.DB
}

func NewDriver(logger *service.Logger, config *DriverConfig) *Driver {
	conn := protonDriver.OpenDB(&protonDriver.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: protonDriver.Auth{
			Username: config.User,
			Password: config.Password,
		},
		DialTimeout: 5 * time.Second,
	})

	conn.SetMaxIdleConns(config.MaxIdleConns)

	return &Driver{
		logger: logger,
		conn:   conn,
	}
}

func (d *Driver) Write(stream string, cols []string, rows [][]any) error {
	scope, err := d.conn.Begin()
	if err != nil {
		return err
	}

	ctx := context.Background()
	batch, err := scope.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s (%s) values", stream, strings.Join(cols, ",")))
	if err != nil {
		return err
	}

	for _, row := range rows {
		if _, err := batch.Exec(row...); err != nil {
			return err
		}
	}

	return scope.Commit()
}
