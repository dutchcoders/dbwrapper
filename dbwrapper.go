package dbwrapper

import (
	"database/sql"
	"errors"
	"fmt"
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

func (stmt *Stmt) QueryRow(args ...interface{}) *Row {
	rows, err := stmt.Stmt.Query(args...)
	if err != nil {
		return &Row{err: err}
	}
	return &Row{rows: rows}
}

func (db *DB) QueryRow(query string, args ...interface{}) *Row {
	rows, err := db.Query(query, args...)
	return &Row{rows: rows, err: err}
}

type Row struct {
	//	*sql.Row
	err  error // deferred error for easy chaining
	rows *sql.Rows
}

func find(values []string, value string) int {
	for i, v := range values {
		if v != value {
			continue
		}

		return i
	}

	return -1
}

func mapColumns(dest []interface{}, o interface{}, columns []string, prefix string, j *int) error {
	oType := reflect.TypeOf(o)
	if oType.Kind() == reflect.Ptr {
		oType = oType.Elem()
	}

	oValue := reflect.ValueOf(o)
	if oValue.Kind() == reflect.Ptr {
		oValue = oValue.Elem()
	}

	switch oType.Kind() {
	case reflect.Struct:
		for i := 0; i < oType.NumField(); i++ {
			fType := oType.Field(i)
			fValue := oValue.Field(i)

			child := fValue.Addr().Interface()
			switch fValue.Kind() {
			case reflect.Struct:
				if err := mapColumns(dest, child, columns, "", j); err != nil {
					return err
				}
			default:
				tag := fType.Tag.Get("sql")
				if -1 == find(columns, tag) {
					return fmt.Errorf("Could not find column '%s'.\n", tag)
				}

				dest[*j] = child
				*j++
			}
		}
	case reflect.Slice:
		for i := 0; i < oValue.Len(); i++ {
			if err := mapColumns(dest, oValue.Index(i).Interface(), columns, "", j); err != nil {
				return err
			}
		}
	default:
		dest[*j] = o
		*j++
	}

	return nil
}

func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	var columns []string
	var err error
	columns, err = r.rows.Columns()
	if err != nil {
		log.Println(r, err)
		return err
	}

	dest2 := make([]interface{}, len(columns))
	i := 0
	err = mapColumns(dest2, dest, columns, "", &i)

	defer r.rows.Close()
	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	err = r.rows.Scan(dest...)
	if err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	if err := r.rows.Close(); err != nil {
		return err
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

	i := 0
	dest2 := make([]interface{}, len(columns))
	if err = mapColumns(dest2, dest, columns, "", &i); err != nil {
		return err
	}

	err = rs.Rows.Scan(dest2...)
	return err
}
