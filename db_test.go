package truss_test

import (
	"github.com/corverroos/truss"
	"testing"
)

func TestConnectForTesting(t *testing.T) {
	truss.ConnectForTesting(t,"")
}

