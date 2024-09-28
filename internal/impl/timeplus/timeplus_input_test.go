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
		inputConfig := `
url: tcp://localhost:8463
query: "select * from testinput"
username: default
password: 
`

		conf, err := inputConfigSpec.ParseYAML(inputConfig, env)
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

	t.Run("Successful read data from local timeplus", func(t *testing.T) {
		inputConfig := `
url: http://localhost:8000
query: "select * from table(mystream)"
workspace: default
apikey: test
username: hl
password: fds
`

		conf, err := inputConfigSpec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		in, err := newTimeplusInput(conf, service.MockResources())
		require.NoError(t, err)

		err = in.Connect(context.Background())
		require.NoError(t, err)

		for {
			msg, _, err := in.Read(context.Background())
			require.NotNil(t, msg)
			require.NoError(t, err)
		}

		err = in.Close(context.Background())
		require.NoError(t, err)

	})

}
