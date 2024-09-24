package timeplus

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestOutputTimeplus(t *testing.T) {
	env := service.NewEnvironment()

	t.Run("Fail if workspace is empty", func(t *testing.T) {
		outputConfig := `
url: http://localhost:8000
stream: mystream
`
		conf, err := outputConfigSpec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		_, _, _, err = newTimeplusOutput(conf, service.MockResources())
		require.ErrorContains(t, err, "workspace")
	})

	t.Run("Successful send data to local timeplusd", func(t *testing.T) {
		outputConfig := `
url: http://localhost:8000
workspace: default
stream: mystream
`

		conf, err := outputConfigSpec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		out, _, _, err := newTimeplusOutput(conf, service.MockResources())
		require.NoError(t, err)

		err = out.Connect(context.Background())
		require.NoError(t, err)

		content1 := map[string]any{
			"col1": "hello",
			"col2": 5,
			"col3": 50,
		}

		content2 := map[string]any{
			"col1": "world",
			"col2": 10,
			"col3": 100,
		}

		msg1 := service.NewMessage(nil)
		msg1.SetStructured(content1)

		msg2 := service.NewMessage(nil)
		msg2.SetStructured(content2)

		batch := service.MessageBatch{
			msg1,
			msg2,
		}
		err = out.WriteBatch(context.Background(), batch)
		require.NoError(t, err)

		err = out.Close(context.Background())
		require.NoError(t, err)
	})

	t.Run("Successful send data to remote timeplusd", func(t *testing.T) {
		outputConfig := `
url: https://nextgen.timeplus.cloud/
workspace: nextgen
stream: test_rp
apikey: 7v3fHptcgZBBkFyi4qpG1-scsUnrLbLLgA2PFXTy0H-bcqVBF5iPdU3KG1_k
`

		conf, err := outputConfigSpec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		out, _, _, err := newTimeplusOutput(conf, service.MockResources())
		require.NoError(t, err)

		err = out.Connect(context.Background())
		require.NoError(t, err)

		content1 := map[string]any{
			"col1": "hello",
			"col2": 5,
			"col3": false,
			"col4": 3.14,
		}

		content2 := map[string]any{
			"col1": "world",
			"col2": 10,
			"col3": true,
			"col4": 3.1415926,
		}

		msg1 := service.NewMessage(nil)
		msg1.SetStructured(content1)

		msg2 := service.NewMessage(nil)
		msg2.SetStructured(content2)

		batch := service.MessageBatch{
			msg1,
			msg2,
		}
		err = out.WriteBatch(context.Background(), batch)
		require.NoError(t, err)

		err = out.Close(context.Background())
		require.NoError(t, err)
	})

}

func TestOutputTimeplusd(t *testing.T) {
	env := service.NewEnvironment()

	t.Run("Successful ingest data", func(t *testing.T) {
		outputConfig := `
target: timeplusd
url: http://localhost:3218
stream: mystream
username: default
password: 
`

		conf, err := outputConfigSpec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		out, _, _, err := newTimeplusOutput(conf, service.MockResources())
		require.NoError(t, err)

		err = out.Connect(context.Background())
		require.NoError(t, err)

		content1 := map[string]any{
			"col1": "hello",
		}

		content2 := map[string]any{
			"col1": "world",
		}

		msg1 := service.NewMessage(nil)
		msg1.SetStructured(content1)

		msg2 := service.NewMessage(nil)
		msg2.SetStructured(content2)

		batch := service.MessageBatch{
			msg1,
			msg2,
		}
		err = out.WriteBatch(context.Background(), batch)
		require.NoError(t, err)

		err = out.Close(context.Background())
		require.NoError(t, err)
	})

}
