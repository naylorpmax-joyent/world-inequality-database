package migrate

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"go.uber.org/zap"

	"github.com/naylorpmax-joyent/world-inequality-database/pkg/db"
)

type Migrate struct {
	logger *zap.SugaredLogger
	db     *db.DB
}

func New(db *db.DB, logger *zap.SugaredLogger) (*Migrate, error) {
	return &Migrate{
		logger: logger,
		db:     db,
	}, nil
}

func (m *Migrate) Down(ctx context.Context) error {
	m.logger.Debugf("initializing down migration")

	migrater, err := migrate.New("file://migrations", "mysql://"+m.db.ConnString())
	if err != nil {
		return fmt.Errorf("error initializing migration: %w", err)
	}

	m.logger.Debugf("applying down migration")
	if err := migrater.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("error performing down migration: %v", err)
	}

	m.logger.Info("migrations down!")
	return nil
}

func (m *Migrate) Up(ctx context.Context) error {
	m.logger.Debugf("initializing up migration")

	migrater, err := migrate.New("file://migrations", "mysql://"+m.db.ConnString())
	if err != nil {
		return fmt.Errorf("error initializing migration: %w", err)
	}

	m.logger.Debugf("applying up migration")
	if err := migrater.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("error performing up migration: %w", err)
	}

	m.logger.Info("migrations up!")
	return nil
}

func (m *Migrate) Steps(ctx context.Context, delta int) error {
	m.logger.Debugf("applying: %d", delta)

	migrater, err := migrate.New("file://migrations", "mysql://"+m.db.ConnString())
	if err != nil {
		return fmt.Errorf("error initializing migration: %w", err)
	}

	m.logger.Debugf("applying migration version change (delta %d)", delta)
	if err := migrater.Steps(delta); err != nil {
		return fmt.Errorf("error performing migration version change: %w", err)
	}

	version, dirty, err := migrater.Version()
	if err != nil {
		return fmt.Errorf("error checking current migration version: %w", err)
	}
	if dirty {
		return fmt.Errorf("dirty version, cannot apply migration: check schema_migrations table and fix version")
	}

	m.logger.Info("current migration version: %d", version)
	return nil
}
