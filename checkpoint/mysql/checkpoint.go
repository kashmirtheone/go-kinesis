package mysql

import (
	"database/sql"

	"github.com/pkg/errors"
)

// Checkpoint implements a KV store to keep track of processing checkpoints.
type Checkpoint struct {
	conn *sql.DB
}

// NewCheckpoint creates a new checkpoint storage.
func NewCheckpoint(conn *sql.DB) Checkpoint {
	return Checkpoint{
		conn: conn,
	}
}

// Get the key/value.
func (c Checkpoint) Get(key string) (string, error) {
	row := c.conn.QueryRow(`
		SELECT value
		FROM kinesis_checkpoint
		WHERE id = ?
	`, key)

	var value string
	if err := row.Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}

		return "", errors.Wrap(err, "error during query by a key")
	}

	return value, nil
}

// Set the key/value.
func (c Checkpoint) Set(key, value string) error {
	_, err := c.conn.Exec(`
		INSERT INTO kinesis_checkpoint
		VALUES(?, ?)
		ON DUPLICATE KEY UPDATE value = ?
	`, key, value, value)

	if err != nil {
		return errors.New("error during set the key")
	}

	return nil
}
