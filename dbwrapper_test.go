package dbwrapper

import (
	_ "database/sql"
	"reflect"

	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func Test(t *testing.T) {
	dsn := os.Getenv("DSN")
	if dsn == "" {
		t.Errorf("DSN not set")
	}

	db, err := Open("mysql", dsn)
	if err != nil {
		t.Errorf("Test failed with error: %s", err)
	}

	type Comment struct {
		Body string     `sql:"body"`
		Date *time.Time `sql:"date"`
		User struct {
			Name string `sql:"user_name"`
		}
	}

	comments := []Comment{}

	qry := "SELECT body, date, u.name as user_name FROM comments c INNER JOIN users u ON c.userid=u.userid"
	err = db.WithStmt(qry, func(stmt *Stmt) error {
		err = stmt.Query(func(rows *Rows) error {
			var comment Comment
			if err := rows.Scan(&comment.Body, &comment.Date, &comment.User.Name); err != nil {
				return err
			}

			comments = append(comments, comment)
			return nil
		})
		return err
	})

	if err != nil {
		t.Errorf("Test failed with error: %s", err)
	}

	want := []Comment{Comment{Body: "test", Date: nil, User: struct {
		Name string "sql:\"user_name\""
	}{Name: "user test"}}}

	if !reflect.DeepEqual(comments, want) {
		t.Errorf("Test failed :\ngot  :\n%#v\n\nwant :\n%#v\n\n", comments, want)
	}

	comments = []Comment{}

	err = db.WithStmt(qry, func(stmt *Stmt) error {
		err = stmt.Query(func(rows *Rows) error {
			var comment Comment
			if err := rows.Scan(&comment); err != nil {
				return err
			}

			comments = append(comments, comment)
			return nil
		})
		return err
	})

	if err != nil {
		t.Errorf("Test failed with error: %s", err)
	}

	if !reflect.DeepEqual(comments, want) {
		t.Errorf("Test failed :\ngot  :\n%#v\n\nwant :\n%#v\n\n", comments, want)
	}
}
