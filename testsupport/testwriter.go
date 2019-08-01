package testsupport

import (
	"testing"
	"time"

	"github.com/getlantern/golog"
)

type testWriter struct {
	t     *testing.T
	start time.Time
}

func (tw *testWriter) Write(b []byte) (int, error) {
	tw.t.Logf("(+%dms) %v", time.Now().Sub(tw.start).Nanoseconds()/1000000, string(b))
	return len(b), nil
}

// RedirectLogsToTest redirects golog log statements to t.Log. Call the returned cancel function
// to start sending logs back to stdout and stderr.
func RedirectLogsToTest(t *testing.T) (cancel func()) {
	tw := &testWriter{t, time.Now()}
	golog.SetOutputs(tw, tw)
	return func() {
		golog.ResetOutputs()
		time.Sleep(1 * time.Second)
	}
}
