package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/v2"
	"go.uber.org/zap"
)

var (
	UnknownDatabaseErr uint16 = 1049

	PingTimeout  = 5 * time.Second
	PingInterval = 300 * time.Millisecond
)

type DB struct {
	*sqlx.DB
	*zap.SugaredLogger

	config
}

type config struct {
	user     string
	password string
	host     string
	port     int
	database string
}

func Connect(ctx context.Context, logger *zap.SugaredLogger, database string, enableDBLogs bool) (*DB, error) {
	logger.Debug("configuring database connection")

	k := koanf.New(".")
	if err := k.Load(env.Provider("WID_", "_", cleanEnv), nil); err != nil {
		return nil, fmt.Errorf("error configuring database connection from env: %w", err)
	}
	if !enableDBLogs {
		mysql.SetLogger(shh{})
	}

	db := DB{
		config: config{
			user:     k.MustString("db.user"),
			password: k.MustString("db.pass"),
			host:     k.MustString("db.host"),
			port:     k.MustInt("db.port"),
			database: database,
		},
		SugaredLogger: logger,
	}

	// connect to the database
	db.Debugf("connecting to database (%d, %d)", PingTimeout, PingInterval)

	dbx, err := db.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}
	db.DB = dbx

	return &db, nil
}

func (c *DB) connect(ctx context.Context) (*sqlx.DB, error) {
	dbxCh := make(chan *sqlx.DB, 1)
	errCh := make(chan error, 1)

	go func() {
		var (
			db  *sql.DB
			err error
		)

		for {
			// wait a bit between each attempt
			time.Sleep(PingInterval)

			// try to open the database connection if we haven't been able to yet
			if db == nil {
				db, err = sql.Open("mysql", c.ConnString())

				if err != nil {
					c.Debugf("error opening database connection: %v", err)
					continue
				}
			}

			// ping the database once we have a connection
			err := db.PingContext(ctx)
			var mysqlErr *mysql.MySQLError
			if errors.As(err, &mysqlErr) && mysqlErr.Number == UnknownDatabaseErr {
				errCh <- fmt.Errorf("database %s does not exist", c.database)
				return
			}
			if err != nil {
				c.Debugf("error pinging database: %v", err)
				continue
			}

			dbxCh <- sqlx.NewDb(db, "mysql")
			return
		}
	}()

	// wait for either the database ping to succeed or to timeout

	select {
	case <-time.After(PingTimeout):
		return nil, fmt.Errorf("timed out pinging database %s after %d", c.database, PingTimeout)
	case err := <-errCh:
		return nil, err
	case db := <-dbxCh:
		return db, nil
	}
}

func (c *DB) ConnString() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.user, c.password, c.host, c.port, c.database)
}

func cleanEnv(s string) string {
	s = strings.TrimPrefix(s, "WID_")
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "_", ".")
	return s
}

type shh struct{}

func (s shh) Print(args ...interface{}) {}
