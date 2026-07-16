// Package ledger manages the SQLite-backed credit ledger.
// Credits are denominated in tokens (1 credit = 1 output token).
// New accounts receive SIGNUP_BONUS credits.
package ledger

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

const SIGNUP_BONUS int64 = 50_000

// Ledger wraps the SQLite database.
type Ledger struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite ledger at path.
func Open(path string) (*Ledger, error) {
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("open ledger: %w", err)
	}
	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate ledger: %w", err)
	}
	return &Ledger{db: db}, nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS accounts (
			token_hash  TEXT PRIMARY KEY,
			balance     INTEGER NOT NULL DEFAULT 0,
			earned      INTEGER NOT NULL DEFAULT 0,
			spent       INTEGER NOT NULL DEFAULT 0,
			rating_sum  INTEGER NOT NULL DEFAULT 0,
			rating_cnt  INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS jobs (
			job_id          TEXT PRIMARY KEY,
			consumer_hash   TEXT NOT NULL,
			provider_hash   TEXT NOT NULL,
			tokens_generated INTEGER NOT NULL DEFAULT 0,
			rating          INTEGER,
			created_at      INTEGER NOT NULL DEFAULT (unixepoch())
		);
	`)
	return err
}

// Balance returns the balance, earned, and spent totals for a token hash.
// If the account does not exist it is created with the signup bonus.
func (l *Ledger) Balance(tokenHash string) (balance, earned, spent int64, err error) {
	row := l.db.QueryRow(`SELECT balance, earned, spent FROM accounts WHERE token_hash = ?`, tokenHash)
	err = row.Scan(&balance, &earned, &spent)
	if err == sql.ErrNoRows {
		_, err = l.db.Exec(
			`INSERT INTO accounts (token_hash, balance, earned) VALUES (?, ?, ?)`,
			tokenHash, SIGNUP_BONUS, SIGNUP_BONUS,
		)
		return SIGNUP_BONUS, SIGNUP_BONUS, 0, err
	}
	return
}

// Settle records a completed job: debits the consumer, credits the provider.
// tokens is the eval_count from the Ollama done event.
func (l *Ledger) Settle(jobID, consumerHash, providerHash string, tokens int64) error {
	tx, err := l.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(
		`INSERT OR IGNORE INTO accounts (token_hash) VALUES (?)`, consumerHash)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT OR IGNORE INTO accounts (token_hash) VALUES (?)`, providerHash)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`UPDATE accounts SET balance = balance - ?, spent = spent + ? WHERE token_hash = ?`,
		tokens, tokens, consumerHash)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`UPDATE accounts SET balance = balance + ?, earned = earned + ? WHERE token_hash = ?`,
		tokens, tokens, providerHash)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT INTO jobs (job_id, consumer_hash, provider_hash, tokens_generated)
		 VALUES (?, ?, ?, ?)`,
		jobID, consumerHash, providerHash, tokens)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Rate records a consumer rating (1–5) for a job.
func (l *Ledger) Rate(jobID string, rating int) error {
	_, err := l.db.Exec(
		`UPDATE jobs SET rating = ? WHERE job_id = ?`, rating, jobID)
	if err != nil {
		return err
	}
	// Update the provider's aggregate rating.
	_, err = l.db.Exec(`
		UPDATE accounts SET
			rating_sum = rating_sum + ?,
			rating_cnt = rating_cnt + 1
		WHERE token_hash = (SELECT provider_hash FROM jobs WHERE job_id = ?)`,
		rating, jobID)
	return err
}

// SuspendCheck returns true if the provider's last 3 ratings average below 2.
// The relay should stop forwarding jobs to suspended providers.
func (l *Ledger) SuspendCheck(providerHash string) (bool, error) {
	row := l.db.QueryRow(`
		SELECT COUNT(*), COALESCE(AVG(rating), 5)
		FROM (
			SELECT rating FROM jobs
			WHERE provider_hash = ? AND rating IS NOT NULL
			ORDER BY created_at DESC LIMIT 3
		)`, providerHash)
	var cnt int
	var avg float64
	if err := row.Scan(&cnt, &avg); err != nil {
		return false, err
	}
	return cnt >= 3 && avg < 2.0, nil
}
