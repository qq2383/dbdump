package lib

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

type DbImport struct {
	reader *bufio.Reader
	Over   bool
}

func NewDbImport(cli *Cli) {
	f, err := os.OpenFile(cli.Path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	dbi := &DbImport{reader: bufio.NewReader(f)}

	conn, err := NewConn(cli)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		dbi.Do(conn, cli.Pros, wg)
	}(&wg)

	dbi.Parse()

	wg.Wait()
	conn.Close()
}

func (dbi *DbImport) Parse() {
	for {
		line, err := dbi.read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		dbi.parse(line)
	}
	if th != nil {
		th.Type = "End"
	}
	dbi.Over = true
	fmt.Printf("dbi tables: %d\n", len(datas))
}

func (dbi *DbImport) read() (string, error) {
	return dbi.reader.ReadString('\n')
}

func (dbi *DbImport) parse(line string) {
	if strings.HasPrefix(line, "DROP TABLE ") {
		if th != nil {
			th.Type = "End"
		}

		ws := strings.Split(line, " ")
		tableName := dbi.parseStr(ws[4])
		fmt.Printf("dbi drop %s\n", tableName)

		th = NewTableHandle(tableName)
		datas[tableName] = th
		que.Put(th)

		th.DoType = "Drop"
		th.Drop = dbi.parseStr(line)

	} else if strings.HasPrefix(line, "CREATE TABLE ") {
		if th != nil {
			fmt.Printf("dbi create %s\n", th.Name)
			query, keys := dbi.parseKey(line)
			th.Crate = strings.Join(query, "")
			th.Keys = keys
		}

	} else if strings.HasPrefix(line, "INSERT INTO ") {
		if th != nil {
			if th.Insert == "" {
				th.ParseInsert()
			}
			query := dbi.parseInsert(line, th.Insert)
			th.Add(query)
		}
	}
}

func (dbi *DbImport) parseKey(str string) (query []string, keys []string) {
	query = append(query, dbi.parseStr(str))
	for {
		str, _ = dbi.read()
		if strings.HasPrefix(str, "  KEY ") || strings.HasPrefix(str, "  INDEX ") {
			str = dbi.parseStr(str)
			str = strings.Replace(str, "KEY", "", 1)
			str = strings.Replace(str, "INDEX", "", 1)
			str = strings.Trim(str, " ")
			keys = append(keys, dbi.parseStr(str))
		} else {
			query = append(query, dbi.parseStr(str))
			if rege.MatchString(str) {
				p := len(query) - 2
				endStr := query[p]
				if strings.HasSuffix(endStr, ",") {
					query[p] = endStr[0 : len(endStr)-1]
				}
				break
			}
		}
	}
	return query, keys
}

func (dbi *DbImport) parseInsert(str string, insert string) string {
	query := make([]string, 0)
	for {
		if strings.Index(str, insert) == 0 {
			str = strings.TrimPrefix(str, insert)
		}
		query = append(query, dbi.parseStr(str))
		if rege.MatchString(str) {
			break
		}
		str, _ = dbi.read()
	}
	return strings.Join(query, "")
}

func (dbi *DbImport) parseStr(str string) string {
	_str := regn.ReplaceAllString(str, "")
	return rege.ReplaceAllString(_str, "")
}

func (dbi *DbImport) Do(conn *Conn, ps int, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		fmt.Printf("dbi do over\n", )
	}()

	handle := func(ch chan int) {
		for {
			if que.Font() == nil {
				if dbi.Over {
					break
				}
				time.Sleep(time.Millisecond * 20)
				continue
			}
			th := que.Pop().(*TableHandle)
			fmt.Printf("dbi do: %s\n", th.Name)

			th.Import(conn)
		}
		ch <- 1
	}

	ch := make(chan int, ps)
	for i := 0; i < ps; i++ {
		go handle(ch)
	}

	for i := 0; i < ps; i++ {
		<-ch
	}
}
