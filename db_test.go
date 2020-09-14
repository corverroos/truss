package truss_test

import (
	"flag"
	"testing"

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
