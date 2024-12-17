package lib

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DbExport struct {
	tbs  []string
	Over bool
}

func NewDbExport(cli *Cli) {
	conn, err := NewConn(cli)
	if err != nil {
		panic(err)
	}

	dbe := &DbExport{}
	dbe.tbs = cli.Tbs

	fp := path.Dir(cli.Path)
	fn := path.Base(cli.Path)

	ext := path.Ext(fn)
	temd := strings.TrimSuffix(fn, ext)	
	temp := path.Join(fp, "~" + temd)

	if _, err := os.Stat(temp); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(temp, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		dbe.Do(conn, cli.Pros, temp, wg)
	}(&wg)

	dbe.tables(conn)
	dbe.do()

	wg.Wait()
	dbe.merge(temp, cli.Path)
	conn.Close()

}

func (dbe *DbExport) tables(conn *Conn) {
	if len(dbe.tbs) == 0 {
		rows := conn.Query("show tables")
		for rows.Next() {
			var tn string
			rows.Scan(&tn)
			dbe.tbs = append(dbe.tbs, tn)
		}
	}
	fmt.Printf("dbe tbs: %d\n", len(dbe.tbs))
}

func (dbe *DbExport) Do(conn *Conn, ps int, dir string, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		fmt.Printf("dbe do over\n", )
	}()

	handle := func(dir string, ch chan int) {
		for {
			if que.Font() == nil {
				if dbe.Over {
					break
				}
				time.Sleep(time.Millisecond * 20)
				continue
			}
			th := que.Pop().(*TableHandle)
			fmt.Printf("dbe do: %s\n", th.Name)

			th.DoType = "DROP TABLE"
			th.Export(conn, dir)
		}
		ch <- 1
	}

	ch := make(chan int, ps)
	for i := 0; i < ps; i++ {
		go handle(dir, ch)
	}

	for i := 0; i < ps; i++ {
		<-ch
	}
}

func (dbe *DbExport) do() {
	for i, tb := range dbe.tbs {
		th := NewTableHandle(tb)
		th.Index =i
		que.Put(th)
	}
	dbe.Over = true
}

func (dbe *DbExport) merge(dir string, fp string) {
	d, err := os.Open(dir)
    if err != nil {
        panic(err)
    }

    fs, err := d.Readdir(-1)
    d.Close()
    if err != nil {
        panic(err)
    }
    sort.Slice(fs, func(i, j int) bool { 
		n1, _ := strconv.Atoi(fs[i].Name())
		n2, _ := strconv.Atoi(fs[j].Name())
		return n1 < n2 
	})

	des, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
    if err != nil {
        panic(err)
    }
	w := bufio.NewWriter(des)
	for _, f := range fs {
		p := path.Join(dir, f.Name())
		buf, err := os.ReadFile(p)
		if err != nil {
			panic(err)
		}
		w.Write(buf)
	}

	os.RemoveAll(dir)
}
