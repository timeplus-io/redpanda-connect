package timeplus

import "context"

type Writer interface {
	Write(ctx context.Context, cols []string, rows [][]any) error
}

type Reader interface {
	Run(ctx context.Context, sql string) error
	Read(ctx context.Context) (map[string]any, error)
	Close() error
}
