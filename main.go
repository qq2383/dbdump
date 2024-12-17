package main

import (
	"fmt"
	"time"

	"github.com/qq2383/dbdump/lib"
)

func main() {
	cli, err := lib.NewCli()
	if err != nil {
		panic(err)
	}
	
	start := time.Now()

	if cli.Dir == ">" {
		lib.NewDbExport(cli)
	} else if cli.Dir == "<" {
		lib.NewDbImport(cli)
	}

	diff := time.Since(start)
	t := time.Unix(int64(diff.Seconds()), diff.Nanoseconds())
	fmt.Printf("over time: %s\n", t.UTC().Format("15:04:05"))
}
