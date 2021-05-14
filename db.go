package truss

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

var sessionUTC = flag.Bool("truss_utc_session", true, "Sets each new DB session timezone to UTC if true")

func Connect(connectStr string) (*sql.DB, error) {
	const prefix = "mysql://"
	if !strings.HasPrefix(connectStr, prefix) {
		return nil, errors.New("connect string missing mysql:// prefix")
	}
	connectStr = connectStr[len(prefix):]

	if !strings.Contains(connectStr, "?") {
		connectStr += "?"
	}
	if !strings.HasSuffix(connectStr, "?") {
		connectStr += "&"
	}

	connectStr += defaultOptions()

	return sql.OpenDB(connector{dns: connectStr}), nil
}

type connector struct {
	driver mysql.MySQLDriver
	dns    string
}

func (c connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dns)
	if err != nil {
		return nil, err
	}

	if !*sessionUTC {
		return conn, nil
	}

	stmt, err := conn.Prepare("set time_zone='+00:00';")
	if err != nil {
		return nil, err
	}

	_, err = stmt.Exec(nil)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (c connector) Driver() driver.Driver {
	return c.driver
}

// ConnectForTesting returns a connection to a newly created database
// with migration queries applied. Test cleanup automatically drops the database.
func ConnectForTesting(t *testing.T, queries ...string) *sql.DB {
	ctx := context.Background()

	uri := getTestURI()

	dbc, err := Connect(uri)
	jtest.RequireNil(t, err)

	dbName := fmt.Sprintf("truss_%d", time.Now().UnixNano())

	_, err = dbc.ExecContext(ctx, "CREATE DATABASE "+dbName+" CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;")
	jtest.RequireNil(t, err)

	uri += dbName

	dbc, err = Connect(uri)
	jtest.RequireNil(t, err)

	// Multiple connections are problematic for unit tests since they
	// introduce concurrency issues.
	dbc.SetMaxOpenConns(1)
	dbc.SetMaxIdleConns(1)

	err = Migrate(ctx, dbc, queries)
	jtest.RequireNil(t, err)

	t.Cleanup(func() {
		defer dbc.Close()

		// Best effort cleanup any old truss test DBs that are still around.
		_, err = dbc.ExecContext(ctx, "DROP DATABASE "+dbName+";")
		if err != nil {
			// NoReturnErr: Best effort, just return
			return
		}

		dl, err := queryStrings(ctx, dbc, "SHOW DATABASES")
		if err != nil {
			// NoReturnErr: Best effort, just return
			return
		}

		for _, d := range dl {
			if !strings.HasPrefix(d, "truss_") {
				continue
			}
			nano, err := strconv.ParseInt(strings.TrimPrefix(d, "truss_"), 10, 64)
			if err != nil {
				// NoReturnErr: Best effort, just return
				continue
			}
			if time.Since(time.Unix(0, nano)) < time.Hour*24 {
				// Only cleanup very old DBs (avoid races).
				continue
			}
			_, err = dbc.ExecContext(ctx, "DROP DATABASE "+d+";")
			if err != nil {
				// NoReturnErr: Best effort, just return
				return
			}
		}
	})

	return dbc
}

// TestSchema ensures that the schema file is up to date with the queries. It updates
// it if update is true.
func TestSchema(t *testing.T, schemapath string, update bool, queries ...string) {
	dbc := ConnectForTesting(t)
	ctx := context.Background()

	err := Migrate(ctx, dbc, queries)
	jtest.RequireNil(t, err)

	schema, err := MakeCreateSchema(ctx, dbc)
	jtest.RequireNil(t, err)

	if update {
		err := ioutil.WriteFile(schemapath, []byte(schema), 0644)
		jtest.RequireNil(t, err)
		return
	}

	actual, err := ioutil.ReadFile(schemapath)
	jtest.RequireNil(t, err)

	require.Equal(t, schema, string(actual))
}

func defaultOptions() string {
	// parseTime: Allows using time.Time for datetime
	return "parseTime=true"
}

func sockFile() string {
	sock := "/tmp/mysql.sock"
	if _, err := os.Stat(sock); os.IsNotExist(err) {
		// try common linux/Ubuntu socket file location
		return "/var/run/mysqld/mysqld.sock"
	}
	return sock
}

const envTestURI = "TRUSS_TEST_URI" // Needs to be in format: mysql://user:password@protocol(address)/

func getTestURI() string {
	if uri, ok := os.LookupEnv(envTestURI); ok {
		return uri
	}

	return "mysql://root@unix(" + sockFile() + ")/"
}
