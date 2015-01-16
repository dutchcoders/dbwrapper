package dbwrapper

import (
	"database/sql"
	"log"
	"reflect"
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

func (tx *Tx) WithStmt(query string, fn func(stmt *Stmt) error) error {
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

func (stmt *Stmt) Query(rowFn func(rows *Rows) error, args ...interface{}) error {
	var rows *sql.Rows
	var err error

	if rows, err = stmt.Stmt.Query(args...); err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		err = rowFn(&Rows{rows})

		if err != nil {
			return err
		}
	}

	return nil
}

type Rows struct {
	*sql.Rows
}

func (rs *Rows) Scan(dest ...interface{}) error {

	var columns []string
	var err error
	columns, err = rs.Rows.Columns()
	if err != nil {
		return err
	}

	o := dest[0]
	st := reflect.TypeOf(o).Elem()
	if st.Kind() == reflect.Struct {
		// check for pointer of struct
		dest = make([]interface{}, len(columns))
		for j := 0; j < len(columns); j++ {
			for i := 0; i < st.NumField(); i++ {
				field := st.Field(i)
				tag := field.Tag.Get("sql")
				if tag != columns[j] {
					continue
				}
				dest[j] = reflect.ValueOf(o).Elem().Field(i).Addr().Interface()
			}
		}
	}

	err = rs.Rows.Scan(dest...)
	return err
}
