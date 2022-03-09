package processor

import (
	"strconv"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/log"
	"github.com/Jeffail/benthos/v3/internal/manager/mock"
	"github.com/Jeffail/benthos/v3/internal/message"
	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJQAllParts(t *testing.T) {
	conf := NewConfig()
	conf.Type = "jq"
	conf.JQ.Query = ".foo.bar"

	jSet, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	msgIn := message.QuickBatch([][]byte{
		[]byte(`{"foo":{"bar":0}}`),
		[]byte(`{"foo":{"bar":1}}`),
		[]byte(`{"foo":{"bar":2}}`),
	})
	msgs, res := jSet.ProcessMessage(msgIn)
	require.Nil(t, res)
	require.Len(t, msgs, 1)
	for i, part := range message.GetAllBytes(msgs[0]) {
		assert.Equal(t, strconv.Itoa(i), string(part))
	}
}

func TestJQValidation(t *testing.T) {
	conf := NewConfig()
	conf.Type = "jq"
	conf.JQ.Query = ".foo.bar"

	jSet, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	msgIn := message.QuickBatch([][]byte{[]byte("this is bad json")})
	msgs, res := jSet.ProcessMessage(msgIn)

	require.Nil(t, res)
	require.Len(t, msgs, 1)

	assert.Equal(t, "this is bad json", string(message.GetAllBytes(msgs[0])[0]))
}

func TestJQMutation(t *testing.T) {
	conf := NewConfig()
	conf.Type = "jq"
	conf.JQ.Query = `{foo: .foo} | .foo.bar = "baz"`

	jSet, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	ogObj := gabs.New()
	ogObj.Set("is this", "foo", "original", "content")
	ogObj.Set("remove this", "bar")
	ogExp := ogObj.String()

	msgIn := message.QuickBatch(make([][]byte, 1))
	msgIn.Get(0).SetJSON(ogObj.Data())
	msgs, res := jSet.ProcessMessage(msgIn)
	require.Nil(t, res)
	require.Len(t, msgs, 1)

	assert.Equal(t, `{"foo":{"bar":"baz","original":{"content":"is this"}}}`, string(message.GetAllBytes(msgs[0])[0]))
	assert.Equal(t, ogExp, ogObj.String())
}

func TestJQ(t *testing.T) {
	type jTest struct {
		name   string
		path   string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "select obj",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":{"baz":1}}}`,
			output: `{"baz":1}`,
		},
		{
			name:   "select array",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":["baz","qux"]}}`,
			output: `["baz","qux"]`,
		},
		{
			name:   "select obj as str",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":"{\"baz\":1}"}}`,
			output: `"{\"baz\":1}"`,
		},
		{
			name:   "select str",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":"hello world"}}`,
			output: `"hello world"`,
		},
		{
			name:   "select float",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":0.123}}`,
			output: `0.123`,
		},
		{
			name:   "select int",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":123}}`,
			output: `123`,
		},
		{
			name:   "select bool",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":true}}`,
			output: `true`,
		},
		{
			name:   "null result",
			path:   ".baz.qux",
			input:  `{"foo":{"bar":true}}`,
			output: `null`,
		},
		{
			name:   "empty string",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":""}}`,
			output: `""`,
		},
		{
			name:   "convert to csv",
			path:   "[.ts,.id,.msg] | @csv",
			input:  `{"id":"1054fe28","msg":"sample \"log\"","ts":1641393111}`,
			output: `"1641393111,\"1054fe28\",\"sample \"\"log\"\"\""`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "jq"
			conf.JQ.Query = test.path

			jSet, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
			require.NoError(t, err)

			inMsg := message.QuickBatch(
				[][]byte{
					[]byte(test.input),
				},
			)
			msgs, _ := jSet.ProcessMessage(inMsg)
			require.Len(t, msgs, 1)
			assert.Equal(t, test.output, string(message.GetAllBytes(msgs[0])[0]))
		})
	}
}

func TestJQ_OutputRaw(t *testing.T) {
	type jTest struct {
		name   string
		path   string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "select obj",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":{"baz":1}}}`,
			output: `{"baz":1}`,
		},
		{
			name:   "select array",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":["baz","qux"]}}`,
			output: `["baz","qux"]`,
		},
		{
			name:   "select obj as str",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":"{\"baz\":1}"}}`,
			output: `{"baz":1}`,
		},
		{
			name:   "select str",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":"hello world"}}`,
			output: `hello world`,
		},
		{
			name:   "select float",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":0.123}}`,
			output: `0.123`,
		},
		{
			name:   "select int",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":123}}`,
			output: `123`,
		},
		{
			name:   "select bool",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":true}}`,
			output: `true`,
		},
		{
			name:   "null result",
			path:   ".baz.qux",
			input:  `{"foo":{"bar":true}}`,
			output: `null`,
		},
		{
			name:   "empty string",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":""}}`,
			output: ``,
		},
		{
			name:   "convert to csv",
			path:   "[.ts,.id,.msg] | @csv",
			input:  `{"id":"1054fe28","msg":"sample \"log\"","ts":1641393111}`,
			output: `1641393111,"1054fe28","sample ""log"""`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "jq"
			conf.JQ.Query = test.path
			conf.JQ.OutputRaw = true

			jSet, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
			require.NoError(t, err)

			inMsg := message.QuickBatch(
				[][]byte{
					[]byte(test.input),
				},
			)
			msgs, _ := jSet.ProcessMessage(inMsg)
			require.Len(t, msgs, 1)
			assert.Equal(t, test.output, string(message.GetAllBytes(msgs[0])[0]))
		})
	}
}