package test

import (
	"bufio"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestImport(t *testing.T) {
	start := time.Now()

	pp := "H:\\program\\mariadb-10.11.7-winx64\\bin\\mysql.exe"
	dp := "H:\\dbs\\chjz\\chjz_24.11.25_1455.sql"
	
	cmd := exec.Command("cmd")
	w, _ := cmd.StdinPipe()
	out, _ := cmd.StdoutPipe()

	cmd.Start()
	str := fmt.Sprintf("%s -u root import_test < %s\n", pp, dp)
	n, err := w.Write([]byte(str))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("w: %d\n", n)

	go func() {
		wait := false
		r := bufio.NewReader(out)
		for {
			line, _ := r.ReadString('\n')
			if strings.HasPrefix(line, "h:\\mydisk") {
				wait = true
				fmt.Println(line)
			}
			if wait && line == "\r\n" {
				fmt.Println(line == "\r\n")
				w.Write([]byte("exit\n"))
			}
		}
	}()

	cmd.Wait()
	
	diff := time.Since(start)
	difft := time.Unix(int64(diff.Seconds()), diff.Nanoseconds())
	fmt.Printf("over time: %s\n", difft.UTC().Format("15:04:05"))
}