package truss

import (
	"context"
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

func TestMigrateEmpty(t *testing.T) {
	ConnectForTesting(t)
}

func TestMigrateBasic(t *testing.T) {
	ql := []string{
		"CREATE TABLE test1 (id BIGINT, name VARCHAR(255));",
		"CREATE TABLE test2 (id BIGINT, name VARCHAR(255), PRIMARY KEY(id));",
		"CREATE TABLE test3 (id TINYINT, name CHAR(3), PRIMARY KEY(id));",
	}

	dbc1 := ConnectForTesting(t, ql...)
	ctx := context.Background()

	ml1, err := listMigrations(ctx, dbc1)
	jtest.RequireNil(t, err)
	require.Len(t, ml1, 3)

	sh, err := schemaHash(ctx, dbc1)
	jtest.RequireNil(t, err)
	require.Equal(t, ml1[2].SchemaHash, sh)

	dbc2 := ConnectForTesting(t)

	for i := 0; i < len(ql); i++ {
		err := Migrate(ctx, dbc2, ql[:i+1])
		jtest.RequireNil(t, err)
	}

	ml2, err := listMigrations(ctx, dbc2)
	jtest.RequireNil(t, err)
	require.Len(t, ml2, 3)

	require.Equal(t, ml1[2].SchemaHash, ml2[2].SchemaHash)
}

func TestApplyMigration(t *testing.T) {
	q := "CREATE TABLE test1 (id BIGINT, name VARCHAR(255));"

	dbc := ConnectForTesting(t)
	ctx := context.Background()

	err := applyMigration(ctx, dbc, q)
	jtest.RequireNil(t, err)

	ml, err := listMigrations(ctx, dbc)
	jtest.RequireNil(t, err)
	require.Len(t, ml, 1)

	sh, err := schemaHash(ctx, dbc)
	jtest.RequireNil(t, err)

	require.Equal(t, ml[0].SchemaHash, sh)
}
