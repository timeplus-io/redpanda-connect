package timeplus

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestInputTimeplusd(t *testing.T) {
	env := service.NewEnvironment()

	t.Run("Fail if query is empty", func(t *testing.T) {
		inputConfig := `
url: http://localhost:8000
username: timeplusd
password: pw
`
		conf, err := inputConfigSpec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newTimeplusInput(conf, service.MockResources())
		require.ErrorContains(t, err, "query")
	})

	t.Run("Successful read data from local timeplusd", func(t *testing.T) {
		outputConfig := `
url: tcp://localhost:8463
query: "select * from testinput"
username: default
password: 
`

		conf, err := inputConfigSpec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		in, err := newTimeplusInput(conf, service.MockResources())
		require.NoError(t, err)

		err = in.Connect(context.Background())
		require.NoError(t, err)

		msg, _, err := in.Read(context.Background())
		require.NotNil(t, msg)

		err = in.Close(context.Background())
		require.NoError(t, err)
	})

	t.Run("Successful send data to remote timeplusd", func(t *testing.T) {
		outputConfig := `
url: https://nextgen.timeplus.cloud/
workspace: nextgen
stream: test_rp
apikey: 7v3fHptcgZBBkFyi4qpG1-scsUnrLbLLgA2PFXTy0H-bcqVBF5iPdU3KG1_k
`

		conf, err := inputConfigSpec.ParseYAML(outputConfig, env)
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
