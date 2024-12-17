package lib

import (
	"errors"
	"flag"
	"strings"
)

type Cli struct {
	Host string
	Port int
	User string
	Pwd  string
	Type string
	Pros int
	DbName   string
	Tbs  []string
	Dir  string
	Path string
}

func NewCli() (*Cli, error) {
	cli := &Cli{}
	flag.StringVar(&cli.Host, "h", "localhost", "host default localhost")
	flag.IntVar(&cli.Port, "P", 3306, "Port default 3306")
	flag.StringVar(&cli.User, "u", "root", "User name default root")
	flag.StringVar(&cli.Pwd, "p", "", "Password default ''")
	flag.StringVar(&cli.Type, "t", "full", "Opration type full | add default full")
	flag.IntVar(&cli.Pros, "ps", 5, "Process number default 5")
	flag.Parse()

	err := cli.Parse(flag.Args())
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func (cli *Cli) Parse(args []string) error {
	if len(args) < 3 {
		return errors.New("args error")
	}

	cli.DbName = args[0]
	i := 1
	for ; i < len(args); i++ {
		a := strings.Trim(args[i], " ")
		if a != "" {
			if a == "<" || a == ">" {
				cli.Dir = a
			}
			if cli.Dir == "" {
				cli.Tbs = append(cli.Tbs, a)
			} else if i == len(args)-1 {
				cli.Path = args[i]
			}
		}
	}
	if cli.Dir == "" || cli.DbName == "" || cli.Path == "" {
		return flag.ErrHelp
	} else if cli.Dir == "<" && len(cli.Tbs) != 0 {
		return flag.ErrHelp
	}
	return nil
}
