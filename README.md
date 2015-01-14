# dbwrapper
Basic Golang database wrapper.


## Examples

Simple query
```
    db, err := dbwrapper.Open("mysql", config.DSN)
    if err != nil {
        panic(err.Error())
    }

    qwy := "SELECT body, date FROM comments WHERE objectid=? AND active=1 ORDER BY date DESC"
    err = db.WithStmt(qry, func(stmt *dbwrapper.Stmt) error {
        err = stmt.Query(func(rows *sql.Rows) error {
            var comment comment
            if err := rows.Scan(&comment.Body, &comment.Date); err != nil {
                return err
            }

            response.Comments = append(response.Comments, comment)
            return nil
        }, response.ObjectId)
        return nil
    })

    if err != nil {
        log.Println(err)
        return
    }
```

Transaction, will rollback when an error is being returned.

```
    db, err := dbwrapper.Open("mysql", config.DSN)
    if err != nil {
        panic(err.Error())
    }

    err = db.WithTx(func(tx *dbwrapper.Tx) {
        qry := "INSERT INTO sessions (clientid, sensor, username, password, date, remote_addr, server_addr, cast, start_date, end_date) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        err = tx.WithStmt(qry, func(stmt *Stmt) error {
            _, err = stmt.Exec(session.Sensor, session.Username, session.Password, time.Now(), addr.String(), c.Request.RemoteAddr, session.Cast, session.StartDate, session.EndDate)
            return err
        })

        if err != nil {
            return err
        }
    })
```


## Contributions

Contributions are welcome.

## Creators

**Remco Verhoef**
- <https://twitter.com/remco_verhoef>
- <https://twitter.com/dutchcoders>

## Copyright and license

Code and documentation copyright 2011-2014 Remco Verhoef.

Code released under [the MIT license](LICENSE).
