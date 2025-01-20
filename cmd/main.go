package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"

	"github.com/naylorpmax-joyent/world-inequality-database/pkg/db"
	"github.com/naylorpmax-joyent/world-inequality-database/pkg/load"
	"github.com/naylorpmax-joyent/world-inequality-database/pkg/migrate"
)

func main() {
	timeoutFlag := &cli.DurationFlag{
		Name:  "ping-timeout",
		Value: db.PingTimeout,
		Usage: "timeout for establishing the initial database connection (seconds)",
	}
	intervalFlag := &cli.DurationFlag{
		Name:  "ping-interval",
		Value: db.PingInterval,
		Usage: "interval between attempts to establish the initial database connection (seconds)",
	}

	enableDBLogsFlag := &cli.BoolFlag{
		Name:  "enable-db-logs",
		Value: false,
		Usage: "enable db logs for debugging (true or false)",
	}

	verboseFlag := &cli.BoolFlag{
		Name:  "verbose",
		Value: false,
		Usage: "enable verbose logging for debugging (true or false)",
	}

	cmd := &cli.Command{
		Name: "a World Inequality Database CLI",
		Commands: []*cli.Command{
			{
				Name:  "migrate",
				Usage: "apply migrations to database",
				Flags: []cli.Flag{timeoutFlag, intervalFlag, enableDBLogsFlag, verboseFlag},
				Commands: []*cli.Command{
					{
						Name: "up",
						Action: func(ctx context.Context, c *cli.Command) error {
							pingTimeout := c.Duration("ping-timeout")
							if pingTimeout > 0 {
								db.PingTimeout = pingTimeout
							}
							pingInterval := c.Duration("ping-interval")
							if pingInterval > 0 {
								db.PingInterval = pingInterval
							}

							verbose := c.Bool("verbose")
							logger := logger(verbose)

							enableDBLogs := c.Bool("enable-db-logs")
							db, err := db.Connect(ctx, logger, "raw", enableDBLogs)
							if err != nil {
								return err
							}

							m, err := migrate.New(db, logger)
							if err != nil {
								return err
							}

							return m.Up(ctx)
						},
					},
					{
						Name: "down",
						Action: func(ctx context.Context, c *cli.Command) error {
							pingTimeout := c.Duration("ping-timeout")
							if pingTimeout > 0 {
								db.PingTimeout = pingTimeout
							}
							pingInterval := c.Duration("ping-interval")
							if pingInterval > 0 {
								db.PingInterval = pingInterval
							}

							verbose := c.Bool("verbose")
							logger := logger(verbose)

							enableDBLogs := c.Bool("enable-db-logs")
							db, err := db.Connect(ctx, logger, "raw", enableDBLogs)
							if err != nil {
								return err
							}

							m, err := migrate.New(db, logger)
							if err != nil {
								return err
							}

							return m.Down(ctx)
						},
					},
				},
			},
			{
				Name:  "load",
				Usage: "load data into the database",
				Commands: []*cli.Command{
					{
						Name: "raw",
						Action: func(ctx context.Context, c *cli.Command) error {
							// configure loader
							pingTimeout := c.Duration("ping-timeout")
							if pingTimeout > 0 {
								db.PingTimeout = pingTimeout
							}

							pingInterval := c.Duration("ping-interval")
							if pingInterval > 0 {
								db.PingInterval = pingInterval
							}

							verbose := c.Bool("verbose")
							logger := logger(verbose)

							enableDBLogs := c.Bool("enable-db-logs")
							db, err := db.Connect(ctx, logger, "raw", enableDBLogs)
							if err != nil {
								return err
							}

							loader := load.New(db, logger)

							// `countries` file
							if err := loader.Load(ctx, "country", "./data/WID_countries.csv", 5); err != nil {
								return err
							}

							logger.Infof("loaded countries")

							// `metadata` files
							metadata, err := listDirByPrefix("./data", "WID_metadata")
							if err != nil {
								return err
							}
							if err := loader.LoadConcurrent(ctx, "metadata", metadata, 19); err != nil {
								return err
							}

							logger.Infof("loaded metadata")

							// `data` files
							data, err := listDirByPrefix("./data", "WID_data")
							if err != nil {
								return err
							}

							if err := loader.LoadConcurrent(ctx, "data", data, 7); err != nil {
								return err
							}

							logger.Infof("done")
							return nil
						},
					},
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Println(err)
		return
	}
}

func logger(verbose bool) *zap.SugaredLogger {
	config := zap.NewDevelopmentConfig()

	level := zap.InfoLevel
	if verbose {
		level = zap.DebugLevel
	}
	config.Level = zap.NewAtomicLevelAt(level)

	l, err := config.Build()
	if err != nil {
		panic(err)
	}

	return l.Sugar()
}

func listDirByPrefix(dir, prefix string) ([]string, error) {
	all, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	include := make([]string, 0, len(all))
	for _, f := range all {
		if strings.HasPrefix(f.Name(), prefix) {
			include = append(include, "./data/"+f.Name())
		}
	}

	return include, nil
}

//nolint:unused
func filter(x, no []string) []string {
	m := make(map[string]struct{}, len(no))
	for i := range no {
		m[no[i]] = struct{}{}
	}

	filtered := make([]string, 0, len(x))
	for i := range x {
		if _, ok := m[x[i]]; ok {
			continue
		}
		filtered = append(filtered, x[i])
	}

	return filtered
}
