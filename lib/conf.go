package lib

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

var root = ""

type Database struct {
	Host   string `yaml:"host"`
	Port   int    `yaml:"port"`
	DbName string `yaml:"dbName"`
	User   string `yaml:"user"`
	Pwd    string `yaml:"pwd"`
}

type Thread struct {
	Count int `yaml:"count"`
}

type Config struct {
	Database Database `yaml:"database"`
	Thread   Thread   `yaml:"thread"`
}

func ConfigLoad() *Config {
	root, _ = os.Getwd()
	path := filepath.Join(root, "conf.yaml")
	fmt.Println(path)

	buf, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	cnf := Config{}
	cnf.Database.Host = "localhost"
	cnf.Database.Port = 3306
	cnf.Database.User = "root"
	cnf.Thread.Count = 5
	err = yaml.Unmarshal(buf, &cnf)
	if err != nil {
		panic(err)
	}
	return &cnf
}
