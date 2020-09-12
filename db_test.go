package truss_test

import (
	"github.com/corver/truss"
	"testing"
)

func TestConnectForTesting(t *testing.T) {
	truss.ConnectForTesting(t,"")
}

