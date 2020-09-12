package truss

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"regexp"
	"strings"
	"time"
)

type migration struct {
	ID         int64
	QueryHash  string
	SchemaHash string
	CreatedAt  time.Time
}

var bootstrapQuery = `
CREATE TABLE IF NOT EXISTS migrations (
  id BIGINT NOT NULL AUTO_INCREMENT,
  query_hash CHAR(64) NOT NULL,
  schema_hash CHAR(64) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  
  PRIMARY KEY (id) 
);`

const bootstrapHash = "0829c750a08fe4dbb50240e01935494ccefbeb01fa58915406f0057162bf7237"

func Migrate(ctx context.Context, dbc *sql.DB, queries []string) error {
	_, err := dbc.ExecContext(ctx, bootstrapQuery)
	if err != nil {
		return err
	}

	sh, err := schemaHash(ctx, dbc)
	if err != nil {
		return errors.Wrap(err, "bootstrap schema hash")
	}

	ml, err := listMigrations(ctx, dbc)
	if err != nil {
		return err
	}

	if len(ml) == 0 && sh != bootstrapHash {
		return errors.New("bootstrapping failed, hash mismatch, schema not empty?")
	} else if len(ml) > len(queries) {
		return errors.New( "more migrations than queries")
	} else if len(ml) > 0 && ml[len(ml)-1].SchemaHash != sh {
		return errors.New( "schema hash and last migration mismatch")
	}

	for i, m := range ml {
		if m.QueryHash != s2h(queries[i]) {
			return errors.New( "migration and query mismatch", j.MKV{"i": i})
		}
	}

	for i := len(ml); i < len(queries); i++ {
		err := applyMigration(ctx, dbc, queries[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func applyMigration(ctx context.Context, dbc *sql.DB, query string) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	sh, err := schemaHash(ctx, tx)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO migrations "+
		"(query_hash, schema_hash, created_at) "+
		"VALUES (?, ?, now())", s2h(query), sh)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// s2h returns a hex encoded sha256 hash (len=64) of the provided query.
func s2h(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func schemaHash(ctx context.Context, dbc common) (string, error) {
	schema, err := MakeCreateSchema(ctx, dbc)
	if err != nil {
		return "", err
	}

	return s2h(schema), nil
}

type common interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

var autoincExp = regexp.MustCompile(`\sAUTO_INCREMENT=\d+`)

func MakeCreateSchema(ctx context.Context, dbc common) (string, error) {
	tables, err := queryStrings(ctx, dbc, "SHOW TABLES")
	if err != nil {
		return "", errors.Wrap(err, "show tables")
	}

	var creates []string
	for _, table := range tables {
		var noop, create string
		err := dbc.QueryRowContext(ctx, "SHOW CREATE TABLE "+table).Scan(&noop, &create)
		if err != nil {
			return "", errors.Wrap(err, "show crete table")
		}

		create = strings.TrimSpace(create)
		create = autoincExp.ReplaceAllString(create, "")
		creates = append(creates, create)
	}

	return strings.Join(creates, "\n\n"), nil
}

const cols = " `id`, `query_hash`, `schema_hash`, `created_at` "

func listMigrations(ctx context.Context, dbc *sql.DB) ([]migration, error) {
	rows, err := dbc.QueryContext(ctx, "select "+cols+" from migrations order by id asc")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []migration
	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}

	return res, rows.Err()
}

func queryStrings(ctx context.Context, dbc common, query string) ([]string, error) {
	rows, err := dbc.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []string
	for rows.Next() {
		var r string
		err := rows.Scan(&r)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}

	return res, rows.Err()
}

type rows interface {
	Scan(...interface{}) error
}

func scan(rows rows) (migration, error) {
	var m migration

	err := rows.Scan(&m.ID, &m.QueryHash, &m.SchemaHash, &m.CreatedAt)
	if err != nil {
		return migration{}, err
	}

	return m, nil
}
