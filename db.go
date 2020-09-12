package truss

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

 _ "github.com/go-sql-driver/mysql"
)

func Connect(connectStr string) (*sql.DB, error) {
	const prefix = "mysql://"
	if !strings.HasPrefix(connectStr, prefix) {
		return nil, errors.New("connect string missing mysql:// prefix")
	}
	connectStr = connectStr[len(prefix):]

	if connectStr[len(connectStr)-1] != '?' {
		connectStr += "&"
	}
	connectStr += defaultOptions()

	dbc, err := sql.Open("mysql", connectStr)
	if err != nil {
		return nil, err
	}

	return dbc, nil
}

func ConnectForTesting(t *testing.T, schemapaths ... string) *sql.DB {
	uri := "mysql://root@unix("+sockFile()+")/?"

	dbc, err := Connect(uri)
	jtest.RequireNil(t, err)

	ctx := context.Background()

	// Multiple connections are problematic for unit tests since they
	// introduce concurrency issues.
	dbc.SetMaxOpenConns(1)

	_, err = dbc.ExecContext(ctx, "set time_zone='+00:00';")
	jtest.RequireNil(t, err)

	dbName := fmt.Sprintf("test_%d", time.Now().UnixNano())

	_, err = dbc.ExecContext(ctx, "CREATE DATABASE " + dbName + " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
	jtest.RequireNil(t, err)
	_, err = dbc.ExecContext(ctx, "USE " + dbName + ";")
	jtest.RequireNil(t, err)

	for _, p := range schemapaths {
		schema, err := ioutil.ReadFile(p)
		jtest.RequireNil(t, err)

		for _, q := range strings.Split(string(schema), ";") {
			q = strings.TrimSpace(q)
			if q == "" {
				continue
			}

			_, err = dbc.ExecContext(ctx, q)
			jtest.RequireNil(t, err)
		}
	}

	t.Cleanup(func() {
		_, err = dbc.ExecContext(ctx, "DROP DATABASE " + dbName + ";")
		jtest.RequireNil(t, err)

		jtest.RequireNil(t, dbc.Close())
	})

	return dbc
}


func defaultOptions() string {
	// parseTime: Allows using time.Time for datetime
	// utf8mb4_general_ci: Needed for non-BMP unicode chars (e.g. emoji)
	return "parseTime=true&collation=utf8mb4_general_ci"
}

func sockFile() string {
	sock := "/tmp/mysql.sock"
	if _, err := os.Stat(sock); os.IsNotExist(err) {
		// try common linux/Ubuntu socket file location
		return "/var/run/mysqld/mysqld.sock"
	}
	return sock
}
