package lib

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
	"github.com/qq2383/queue"

)

type TableHandle struct {
	Name   string
	Drop   string
	Crate  string
	Keys   []string
	Insert string
	Que    *queue.Queue
	Type   string
	DoType string
	Index  int
}

func NewTableHandle(tableName string) *TableHandle {
	return &TableHandle{
		Name: tableName,
		Keys: make([]string, 0),
		Que:  &queue.Queue{},
	}
}

func (th *TableHandle) Add(query string) {
	th.Que.Put(query)
}

func (th *TableHandle) ParseInsert() {
	th.Insert = fmt.Sprintf("INSERT INTO %s VALUES", th.Name)
}

func (th *TableHandle) Import(conn *Conn) {
	defer func() {
		fmt.Printf("dbi th done: %s\n", th.Name)
	}()

	for {
		if th.DoType == "Drop" {
			if th.Drop != "" {
				conn.Exec(th.Drop)
			}
			th.DoType = "Create"

		} else if th.DoType == "Create" {
			if th.Crate != "" {
				conn.Exec(th.Crate)
			}
			th.DoType = "Insert"

		} else if th.DoType == "Insert" {
			if th.Type == "End" && th.Que.Size() == 0 {
				th.DoType = "Key"
			} else if th.Type == "End" {
				th.insert(conn, th.Que.Size())
			} else {
				th.insert(conn, 1000)
			}
		} else if th.DoType == "Key" {
			th.Key(conn)
			break
		}

		time.Sleep(time.Millisecond * 20)
	}
}

func (th *TableHandle) insert(conn *Conn, count int) {
	if th.Que.Size() == 0 {
		return
	}

	query := make([]string, 0)
	i := 0
	for i < count {
		i++
		que := th.Que.Pop()
		if que == nil {
			break
		}

		q := que.(string)
		query = append(query, q)
		if len(q) > 1024 {
			break
		}
	}
	if len(query) != 0 {
		sql := fmt.Sprintf("%s %s;", th.Insert, strings.Join(query, ","))
		conn.Exec(sql)
	}
}

func (th *TableHandle) Key(conn *Conn) {
	lines := []string{fmt.Sprintf("ALTER TABLE %s ", th.Name)}
	for _, key := range th.Keys {
		key = "	ADD INDEX " + key
		lines = append(lines, key)
	}
	sql := strings.Join(lines, "")
	conn.Exec(sql)
}

func (th *TableHandle) Export(conn *Conn, dir string) {
	defer func() {
		fmt.Printf("dbe th done: %s\n", th.Name)
	}()

	fp := path.Join(dir, fmt.Sprintf("%d", th.Index))
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriter(f)

	th.WriteCreate(conn, w)
	th.WriteInsert(conn, w)
}

func (th *TableHandle) WriteCreate(conn *Conn, w *bufio.Writer) {
	w.Write([]byte(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", th.Name)))
	w.Flush()

	rows := conn.Query(fmt.Sprintf("show CREATE TABLE `%s`", th.Name))
	for rows.Next() {
		ary := conn.Scan(rows)
		w.Write([]byte(fmt.Sprintf("%s;\n", ary[1])))
		w.Flush()
	}
}

func (th *TableHandle) WriteInsert(conn *Conn, w *bufio.Writer) {
	s := fmt.Sprintf("LOCK TABLES `%s` WRITE;\n", th.Name)
	w.Write([]byte(s))
	w.Flush()

	rows := conn.Query(fmt.Sprintf("select * from `%s`", th.Name))
	max := 200
	i := 0
	vs := make([]string, 0)
	for rows.Next() {
		row := conn.Scan(rows)
		cols, _ := rows.ColumnTypes()
		v := th.parseRow(row, cols)
		vs = append(vs, v)
		if i == max {
			s = fmt.Sprintf("INSERT INTO `%s` VALUES\n%s;\n", th.Name, strings.Join(vs, ",\n"))
			w.Write([]byte(s))
			w.Flush()

			vs = make([]string, 0)
			i = -1
		}
		i++
	}
	if len(vs) != 0 {
		s = fmt.Sprintf("INSERT INTO `%s` VALUES\n%s;\n", th.Name, strings.Join(vs, ",\n"))
		w.Write([]byte(s))
		w.Flush()
	}

	w.Write([]byte("UNLOCK TABLES;\n"))
	w.Flush()
}

func (th *TableHandle) parseRow(row []any, cols []*sql.ColumnType) string {
	des := make([]string, len(cols))
	for i, r := range row {
		if r == nil {
			des[i] = "NULL"
			continue
		}
		switch r := r.(type) {
		case string, []uint8:
			s := string(r.([]byte))
			s = strings.ReplaceAll(s, "\\", "\\\\")
			s = strings.ReplaceAll(s, "\"", "\\\"")
			s = strings.ReplaceAll(s, "'", "\\'")
			des[i] = "'" + s + "'"
		case int8, int16, int32, int64:
			des[i] = strconv.FormatInt(r.(int64), 10)
		case uint8, uint16, uint32, uint64:
			des[i] = strconv.FormatUint(r.(uint64), 10)
		case float32, float64:
			des[i] = strconv.FormatFloat(r.(float64), 'f', 10, 64)
		case bool:
			des[i] = strconv.FormatBool(r)
		case time.Time:
			switch cols[i].DatabaseTypeName() {
			case "DATE":
				des[i] = r.Format("'2006-01-02'")
			case "TIME":
				des[i] = r.Format("'15:04:05'")
			default:
				des[i] = r.Format("'2006-01-02 15:04:05'")
			}
		default:
			fmt.Printf("r not type: %v\n", r)
		}
	}
	return "(" + strings.Join(des, ",") + ")"
}
