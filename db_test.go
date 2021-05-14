package truss_test

import (
	"context"
	"flag"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"

	"github.com/corverroos/truss"
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

func TestEarlyClose(t *testing.T) {
	dbc := truss.ConnectForTesting(t)
	jtest.RequireNil(t, dbc.Close())
}

func TestDBTimestamps(t *testing.T) {
	table := `
create table testtime (
  id bigint auto_increment, 
  ts datetime(3) not null, 
  primary key (id)
);`
	dbc := truss.ConnectForTesting(t, table)

	// Ensure we use separate connections.
	dbc.SetMaxOpenConns(100)
	dbc.SetMaxIdleConns(0)

	ctx := context.Background()

	n := 20

	var wg sync.WaitGroup
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			t0 := time.Now()
			res, err := dbc.ExecContext(ctx, "insert into testtime set ts=now(3)")
			jtest.RequireNil(t, err)

			id, err := res.LastInsertId()
			jtest.RequireNil(t, err)

			var ts time.Time
			err = dbc.QueryRowContext(ctx, "select ts from testtime where id=?", id).Scan(&ts)
			jtest.RequireNil(t, err)
			require.InDelta(t, t0.UnixNano(), ts.UnixNano(), 1e9) // Within 1 sec
		}()

		go func() {
			defer wg.Done()

			t0 := time.Now()
			res, err := dbc.ExecContext(ctx, "insert into testtime set ts=?", t0)
			jtest.RequireNil(t, err)

			id, err := res.LastInsertId()
			jtest.RequireNil(t, err)

			var ts time.Time
			err = dbc.QueryRowContext(ctx, "select ts from testtime where id=?", id).Scan(&ts)
			jtest.RequireNil(t, err)
			require.InDelta(t, t0.UnixNano(), ts.UnixNano(), 1e9) // Within 1 sec
		}()
	}

	wg.Wait()
}
