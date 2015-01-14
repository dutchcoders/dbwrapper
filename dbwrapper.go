package dbwrapper

import (
	"database/sql"
	"log"
	"time"
)

type DB struct {
	*sql.DB
	logging bool
}

var LogFn = log.Printf

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	return &DB{DB: db, logging: true}, err
}

func (db *DB) WithStmt(query string, fn func(stmt *Stmt) error) error {
	began := time.Now()

	stmt, err := db.Prepare(query)
	if err != nil {
		return err
	}

	defer stmt.Close()

	err = fn(&Stmt{stmt})

	LogFn("%s %s %s", query, time.Since(began), err)

	return err
}

/*
err = db.withTx(func (tx *sql.Tx) error {
})
*/

func (db *DB) WithTx(fn func(tx *Tx) error) error {
	var err error
	var tx *sql.Tx

	tx, err = db.Begin()
	if err != nil {
		return err
	}

	if err = fn(&Tx{Tx: tx}); err != nil {
		tx.Rollback()
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

type Tx struct {
	*sql.Tx
}

func (tx *Tx) withStmt(query string, fn func(stmt *Stmt) error) error {
	began := time.Now()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	defer stmt.Close()

	err = fn(&Stmt{stmt})

	LogFn("tx: %s %s %s", query, time.Since(began), err)

	return err
}

type Stmt struct {
	*sql.Stmt
}

func (stmt *Stmt) Query(rowFn func(rows *sql.Rows) error, args ...interface{}) error {
	var rows *sql.Rows
	var err error

	if rows, err = stmt.Stmt.Query(args...); err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		err = rowFn(rows)

		if err != nil {
			return err
		}
	}

	return nil
}
