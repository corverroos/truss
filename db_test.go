package truss_test

import (
	"testing"

	"github.com/corverroos/truss"
)

func TestConnectForTesting(t *testing.T) {
	truss.ConnectForTesting(t)
}
