package lib

import (
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Conn struct {
	*sql.DB
}

func NewConn(cli *Cli) (*Conn, error) {
	parm := url.PathEscape("parseTime=true&loc=Asia/Shanghai")
	dbname := url.PathEscape(cli.DbName)
	dns := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", cli.User, cli.Pwd, cli.Host, cli.Port, dbname, parm)

	conn, err := sql.Open("mysql", dns)
	if err != nil {
		return nil, err
	}
	// See "Important settings" section.
	// conn.SetConnMaxLifetime(time.Minute * 3)
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(10)

	return &Conn{conn}, err
}

func (conn *Conn) Close() error {
	return conn.DB.Close()
}

func (conn *Conn) Exec(query string) int64 {
	res, err := conn.DB.Exec(query)
	if err != nil {
		fmt.Println(query)
		panic(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		panic(err)
	}
	return rows
}

func (conn *Conn) Query(query string, args ...string) (rows *sql.Rows) {
	var err error
	if len(args) == 0 {
		rows, err = conn.DB.Query(query)
	} else {
		stmt, err1 := conn.Prepare(query)
		if err1 != nil {
			panic(err1)
		}
		rows, err = stmt.Query(query, args)
		stmt.Close()
	}
	if err != nil {
		panic(err)
	}
	return rows
}

func (conn *Conn) Scan(rows *sql.Rows) []any {
	cols, _ := rows.ColumnTypes()
	vs := make([]any, len(cols))
	vps := make([]any, len(cols))
	for i := range cols {
		vps[i] = &vs[i]
	}
	err := rows.Scan(vps...)
	if err != nil {
		panic(err)
	}

	return vs
}

func (conn *Conn) Row2String(rows *sql.Rows) []string {
	cols, _ := rows.ColumnTypes()
	vs := make([]any, len(cols))
	vps := make([]any, len(cols))
	for i := range cols {
		vps[i] = &vs[i]
	}
	err := rows.Scan(vps...)
	if err != nil {
		panic(err)
	}

	des := make([]string, len(cols))
	for i, col := range cols {
		var v string
		dtn := col.ScanType().Name()
		switch dtn {
		case "string", "NullString":
			if vs[i] == nil {
				v = "NULL"
			} else {
				v = "'" + string(vs[i].([]byte)) + "'"
			}
		case "NullTime":
			if vs[i] == nil {
				v = "NULL"
			} else {
				t, _ := vs[i].(time.Time)
				v = t.Format("'2006-01-02 15:04:05'")
			}
		case "int16", "int32", "int64", "NullInt16", "NullInt32", "NullInt64":
			if vs[i] == nil {
				v = "NULL"
			} else {
				v = strconv.FormatInt(vs[i].(int64), 10)
			}
		case "float32", "float64", "NullFloat64":
			if vs[i] == nil {
				v = "NULL"
			} else {
				v = strconv.FormatFloat(vs[i].(float64), 'f', 10, 64)
			}
		case "bool", "NullBool":
			if vs[i] == nil {
				v = "NULL"
			} else {
				v = strconv.FormatBool(vs[i].(bool))
			}
		default:
			v = string(vs[i].([]byte))
		}
		des[i] = v
	}
	return des
}
