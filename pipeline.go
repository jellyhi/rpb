package rpb

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type PipelineCb func(*redis.Cmd)

type Pipeline struct {
	liner redis.Pipeliner
	cbs   []PipelineCb
	err   error
	dead  bool
}

func NewPipeline(rdb *redis.Client) *Pipeline {
	liner := rdb.Pipeline()
	return &Pipeline{
		liner: liner,
		cbs:   make([]PipelineCb, 0, 8),
	}
}

func (p *Pipeline) Add(args []interface{}, cb PipelineCb) {
	if p.dead {
		panicf("pipeline dead")
	}
	p.liner.Do(context.Background(), args...)
	p.cbs = append(p.cbs, cb)
}

func (p *Pipeline) SetError(err error) {
	p.err = err
}

func (p *Pipeline) Exec() error {
	defer func() {
		p.liner.Close()
		p.dead = true
	}()

	if p.err != nil {
		return p.err
	}

	cmds, err := p.liner.Exec(context.Background())
	if err != nil {
		return err
	}
	for i, cmd := range cmds {
		p.cbs[i](cmd.(*redis.Cmd))
	}
	return nil
}
