package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	tpAPIVersion                = "v1beta2"
	timeplusdDAPIVersion        = "v1"
	targetTimeplus       string = "timeplus"
	targetTimeplusd      string = "timeplusd"
)

type Client struct {
	logger    *service.Logger
	ingestURL *url.URL
	pingURL   *url.URL
	header    http.Header
	client    *http.Client
}

type tpIngest struct {
	Columns []string `json:"columns" binding:"required"`
	Data    [][]any  `json:"data" binding:"required"`
}

func NewClient(logger *service.Logger, target string, baseURL *url.URL, workspace, stream, apikey string) *Client {
	ingestURL, _ := url.Parse(baseURL.String())
	pingURL, _ := url.Parse(baseURL.String())

	header := http.Header{}
	if target == targetTimeplus {
		ingestURL.Path = path.Join(ingestURL.Path, workspace, "api", tpAPIVersion, "streams", stream, "ingest")
		pingURL.Path = path.Join(pingURL.Path, workspace, "api", "info")

		header.Add("Content-Type", "application/json")
		if len(apikey) > 0 {
			header.Add("X-Api-Key", apikey)
		}
	} else if target == targetTimeplusd {
		ingestURL.Path = path.Join(ingestURL.Path, "timeplusd", timeplusdDAPIVersion, "ingest", "streams", stream)
		pingURL.Path = path.Join(pingURL.Path, workspace, "api", "info")
	}

	return &Client{
		logger,
		ingestURL,
		pingURL,
		header,
		&http.Client{},
	}
}

func (c *Client) Write(stream string, cols []string, rows [][]any) error {
	payload := tpIngest{
		Columns: cols,
		Data:    rows,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", c.ingestURL.String(), bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}
	req.Header = c.header

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("failed to ingest, got status code %d", resp.StatusCode)
	}

	resp.Body.Close()

	return nil
}
