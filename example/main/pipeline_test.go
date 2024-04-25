package main

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/jellyhi/rpb"
	"github.com/jellyhi/rpb/example/dbproto"
)

func Benchmark(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", "", 9527),
		Password: "",
		DB:       0,
	})
	defer rdb.Close()
	panicErr := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	uid := uint64(1)
	op := NewDbprotoOrderOP(rdb, uid)
	orders := make([]*dbproto.Order, 0, 100)
	for i := 0; i < 10; i++ {
		orders = append(orders, &dbproto.Order{
			OrderId: fmt.Sprintf("order%d", i),
			Uid:     uint64(i),
			ExtData: "hello",
		})
	}
	panicErr(op.HMSET(orders))

	for i := 0; i < b.N; i++ {
		for i := 0; i < 30; i++ {
			_, err := op.HLEN()
			panicErr(err)
		}
	}
}

func BenchmarkPipeline(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", "", 9527),
		Password: "",
		DB:       0,
	})
	defer rdb.Close()
	panicErr := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	uid := uint64(1)
	op := NewDbprotoOrderOP(rdb, uid)
	orders := make([]*dbproto.Order, 0, 100)
	for i := 0; i < 10; i++ {
		orders = append(orders, &dbproto.Order{
			OrderId: fmt.Sprintf("order%d", i),
			Uid:     uint64(i),
			ExtData: "hello",
		})
	}
	panicErr(op.HMSET(orders))

	for i := 0; i < b.N; i++ {
		pipeline := rpb.NewPipeline(rdb)
		for i := 0; i < 30; i++ {
			op.PipelineHLEN(pipeline, func(length int, err error) { panicErr(err) })
		}
		panicErr(pipeline.Exec())
	}
}
