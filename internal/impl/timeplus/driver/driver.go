package driver

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	driver "github.com/timeplus-io/proton-go-driver/v2"
)

type DriverImpl struct {
	logger      *service.Logger
	conn        *sql.DB
	rows        *sql.Rows
	columnTypes []*sql.ColumnType
}

var (
	codeRe = *regexp.MustCompile(`code: (.+[0-9])`)
	msgRe  = *regexp.MustCompile(`message: (.*)`)
)

func NewDriver(logger *service.Logger, addr, username, password string) *DriverImpl {
	conn := driver.OpenDB(&driver.Options{
		Addr: []string{addr},
		Auth: driver.Auth{
			Username: username,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})

	return &DriverImpl{
		logger: logger,
		conn:   conn,
	}
}

func (d *DriverImpl) Run(ctx context.Context, sql string) error {
	ckCtx := driver.Context(ctx)
	rows, err := d.conn.QueryContext(ckCtx, sql)
	if err != nil {
		return err
	}

	d.rows = rows

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	d.columnTypes = columnTypes

	return nil
}

func (d *DriverImpl) Read(ctx context.Context) (map[string]any, error) {
	for { // retry loop
		if d.rows.Next() {
			count := len(d.columnTypes)

			values := make([]any, count)
			valuePtrs := make([]any, count)

			for i := range d.columnTypes {
				valuePtrs[i] = &values[i]
			}

			if err := d.rows.Scan(valuePtrs...); err != nil {
				return nil, err
			}

			event := make(map[string]any)
			for i, col := range d.columnTypes {
				event[col.Name()] = values[i]
			}

			return event, nil
		}

		if err := d.rows.Err(); err != nil {
			if errors.Is(err, context.Canceled) || IsQueryCancelErr(err) {
				// Most likely timeplusd got restarted. Since we are going to re-connect to timeplusd once it recovered, we do not log it as error for now.
				d.logger.With("reason", err).Info("query cancelled")
				return nil, io.EOF
			}

			d.logger.With("error", err).Errorf("query failed: %s", err.Error())
			// this happens when the SQL is updated, i.e. a new MV is created, the previous checkpoint is on longer available.
			if strings.Contains(err.Error(), "code: 2003") {
				continue // retry
			}
			return nil, err
		}
		return nil, io.EOF
	}
}

func (d *DriverImpl) Close() error {
	if d.rows != nil {
		d.rows.Close()
	}
	return d.conn.Close()
}

func IsQueryCancelErr(err error) bool {
	code, msg := Parse(err)
	return code == 394 && strings.Contains(msg, "Query was cancelled")
}

func Parse(err error) (int, string) {
	var code int
	var msg string

	errStr := err.Error()
	codeMatches := codeRe.FindStringSubmatch(errStr)
	if len(codeMatches) == 2 {
		code, _ = strconv.Atoi(codeMatches[1])
	}

	msgMatches := msgRe.FindStringSubmatch(errStr)
	if len(msgMatches) == 2 {
		msg = msgMatches[1]
	}

	return code, msg
}
