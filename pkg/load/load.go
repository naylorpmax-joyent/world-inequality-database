package load

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/naylorpmax-joyent/world-inequality-database/pkg/db"
	"go.uber.org/zap"
)

var BatchSize = 2500

type query struct {
	table   string
	columns []string
	values  []map[string]interface{}
}

type Loader struct {
	*db.DB
}

func New(db *db.DB, logger *zap.SugaredLogger) *Loader {
	return &Loader{DB: db}
}

func (l *Loader) Load(ctx context.Context, table, path string, expectedCols int) error {
	reader, err := openCSV(path, expectedCols)
	if err != nil {
		return err
	}

	columns, err := reader.Read()
	if err != nil {
		return fmt.Errorf("error reading for columns for %s: %w", path, err)
	}

	namedArgs := make([]string, len(columns))
	for i := range columns {
		namedArgs[i] = ":" + columns[i]
	}

	var done bool
	var read int

	loaded := 0
	for !done {
		batch := make([]map[string]any, BatchSize)

		var batched int
		for batched < len(batch) {
			record, err := reader.Read()
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("error reading record for %s: %w", path, err)
			}
			if errors.Is(err, io.EOF) {
				done = true
				batch = batch[:batched]
				break
			}
			read++

			// TODO: cleanup
			if columns[4] == "value" && record[4] == "" {
				l.Warnf("missing value in %s at %d", path, read)
				continue
			}

			rowByColumn := make(map[string]any)
			for j := range columns {
				rowByColumn[columns[j]] = record[j]
			}

			batch[batched] = rowByColumn
			batched++
		}

		if len(batch) == 0 {
			return nil
		}

		query := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`,
			table,
			strings.Join(columns, ", "),
			strings.Join(namedArgs, ", "),
		)

		n, err := l.DB.NamedExec(query, batch)
		if err != nil {
			return fmt.Errorf("error executing query to load file %s (batch %d): %w", path, loaded, err)
		}

		affected, err := n.RowsAffected()
		if err != nil {
			return fmt.Errorf("error determining number of affected rows for file %s: %w", path, err)
		}
		if int(affected) != len(batch) {
			return fmt.Errorf("not all records inserted for %s (expected: %d, got %d)", path, len(batch), affected)
		}

		loaded++
	}

	return nil
}

func (l *Loader) LoadConcurrent(ctx context.Context, table string, paths []string, expectedCols int) error {
	maxConcurrency := make(chan struct{}, 12) // TODO: configurable
	done := make(chan error)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		for _, p := range paths {
			maxConcurrency <- struct{}{}
			go func() {
				if err := l.Load(ctx, "data", p, expectedCols); err != nil {
					done <- err
					return
				}

				l.Infof("loaded: %s", p)
				done <- nil
				<-maxConcurrency
			}()
		}
	}()

	for range paths {
		err := <-done
		if err != nil {
			cancel()
			return err
		}
	}

	return nil
}

func openCSV(path string, expectedColumns int) (*csv.Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening file %s: %w", path, err)
	}

	sep, _ := utf8.DecodeRuneInString(";")
	reader := csv.NewReader(f)
	reader.Comma = sep
	reader.FieldsPerRecord = expectedColumns

	return reader, nil
}
