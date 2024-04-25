package main

import (
	"flag"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/jellyhi/rpb"
	"github.com/jellyhi/rpb/example/dbproto"
)

func main() {
	var host string
	var port int
	var password string
	var db int
	flag.StringVar(&host, "host", "0.0.0.0", "Redis host")
	flag.IntVar(&port, "port", 9527, "Redis port")
	flag.StringVar(&password, "password", "", "Redis password")
	flag.IntVar(&db, "db", 0, "Redis db")
	flag.Parse()
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: password,
		DB:       db,
	})
	defer rdb.Close()
	HashOps(rdb)
}

func HashOps(rdb *redis.Client) {
	fmt.Println("------HashOps------")
	defer fmt.Println("------HashOps end------")
	uid := uint64(1)
	op := NewDbprotoOrderOP(rdb, uid)
	defer op.DEL()
	fmt.Println(op.HLEN())
	order1 := &dbproto.Order{OrderId: "order1001", Uid: uid, ExtData: "hello"}
	order2 := &dbproto.Order{OrderId: "order1002", Uid: uid, ExtData: "world"}
	op.HMSET([]*dbproto.Order{order1, order2})
	fmt.Println(op.HLEN())
	fmt.Println(op.HGETALL())
	fmt.Println(op.HKEYS())
	fmt.Println(op.HVALS())

	pipeline := rpb.NewPipeline(rdb)
	op.PipelineHGETALL(pipeline, func(orders map[string]*dbproto.Order, err error) {
		fmt.Println(orders, err)
	})
	pipeline.Exec()
}
