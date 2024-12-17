package lib

import (
	"regexp"

	"github.com/qq2383/queue"
)

var (
	rege, _ = regexp.Compile(`\s*;\r?\n?$`)
	regn, _ = regexp.Compile(`[\n\r]`)

	datas = make(map[string]*TableHandle)
	que   = &queue.Queue{}

	th *TableHandle
)