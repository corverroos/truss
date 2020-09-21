package truss_test

import (
	"context"
	"encoding/hex"
	"flag"
	"math/rand"
	"sync"
	"testing"

	"github.com/corverroos/truss"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update schema file")

//go:generate go test -update -run=TestTestSchema

func TestTestSchema(t *testing.T) {
	ql := []string{
		"CREATE TABLE test1 (id BIGINT, name VARCHAR(255));",
		"CREATE TABLE test2 (id BIGINT, name VARCHAR(255), PRIMARY KEY(id));",
		"CREATE TABLE test3 (id TINYINT, name CHAR(3), PRIMARY KEY(id));",
	}

	if !*update {
		truss.TestSchema(t, "testdata/schema.sql", false, ql...)
	}
	truss.TestSchema(t, "testdata/schema.sql", true, ql...)
	truss.TestSchema(t, "testdata/schema.sql", false, ql...)
}

func TestMultiConnections(t *testing.T) {
	ql := []string{
		"CREATE TABLE test1 (id BIGINT, name VARCHAR(255));",
	}

	dbc := truss.ConnectForTesting(t, ql...)
	dbc.SetMaxOpenConns(100)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			var n int
			err := dbc.QueryRowContext(context.Background(), "select count(1) from test1").Scan(&n)
			jtest.RequireNil(t, err)
			require.Zero(t, n)
			wg.Done()
		}()
	}

	wg.Wait()
}

func genRand(n int) string {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
