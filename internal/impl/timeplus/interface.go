package timeplus

type Writer interface {
	Write(stream string, cols []string, rows [][]any) error
}
