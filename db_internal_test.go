package truss

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

func TestCleanup(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now()
	old := t0.Add(-time.Hour * 24 * 7)
	t.Run("create new and old and clean", func(t *testing.T) {
		dbc := ConnectForTesting(t)
		dbName := fmt.Sprintf("truss_%d", old.UnixNano())

		_, err := dbc.ExecContext(ctx, "CREATE DATABASE "+dbName+" CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;")
		jtest.RequireNil(t, err)
	})
	t1 := time.Now()

	dbc := ConnectForTesting(t)
	dl, err := queryStrings(context.Background(), dbc, "SHOW DATABASES")
	jtest.RequireNil(t, err)

	for _, d := range dl {
		if !strings.HasPrefix(d, "truss_") {
			continue
		}
		nano, err := strconv.ParseInt(strings.TrimPrefix(d, "truss_"), 10, 64)
		jtest.RequireNil(t, err)

		created := time.Unix(0, nano)
		if created.Equal(old) {
			require.Fail(t, "old truss test db not cleaned", d)
		}
		if created.After(t0) && created.Before(t1) {
			require.Fail(t, "truss test db not cleaned", d)
		}
	}
}
