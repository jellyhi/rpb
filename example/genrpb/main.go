package main

import (
	"os"

	"github.com/jellyhi/rpb"
	"github.com/jellyhi/rpb/example/dbproto"
)

func main() {
	rpb.RegisterHash("user", rpb.PrefixNone, &dbproto.User{})
	rpb.RegisterHash("item:", rpb.PrefixUint64, &dbproto.Item{})
	rpb.RegisterHash("order:", rpb.PrefixUint64, &dbproto.Order{})

	writer, err := os.Create("main/gen_rpb.go")
	if err != nil {
		panic(err)
	}
	defer writer.Close()
	rpb.GenerateCode("db", "main", writer)
}
