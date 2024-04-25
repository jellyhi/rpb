
package main

import(
				"fmt"
				"context"
				"strconv"

				"github.com/jellyhi/rpb/example/dbproto"
				

				"github.com/jellyhi/rpb"
				"github.com/gogo/protobuf/proto"
				"github.com/go-redis/redis/v8"
)

func init() {
				rpb.RegisterKeyMeta("db", &rpb.KeyMeta{KeyType: rpb.KeyType("hash"), KeyName:"user", HasPrefix: false,ProtoMessage: "dbproto.User"})
				rpb.RegisterKeyMeta("db", &rpb.KeyMeta{KeyType: rpb.KeyType("hash"), KeyName:"item:", HasPrefix: true,ProtoMessage: "dbproto.Item"})
				rpb.RegisterKeyMeta("db", &rpb.KeyMeta{KeyType: rpb.KeyType("hash"), KeyName:"order:", HasPrefix: true,ProtoMessage: "dbproto.Order"})
}


type DbprotoUserOP struct{
				rdb *redis.Client
				key string
}

func NewDbprotoUserOP(rdb *redis.Client) *DbprotoUserOP {
				key := "user"
				return &DbprotoUserOP{
								rdb: rdb,
								key: key,
				}
}

func (op *DbprotoUserOP)HGETALL() (map[uint64]*dbproto.User, error) {
				return op.decode(op.rdb.Do(context.Background(), "HGETALL", op.key))
}

func (op *DbprotoUserOP)PipelineHGETALL(pipeline *rpb.Pipeline, cb func(map[uint64]*dbproto.User, error)) {
				pipeline.Add([]interface{}{"HGETALL", op.key}, func(cmd *redis.Cmd) {cb(op.decode(cmd))})
}

func (op *DbprotoUserOP)HMGET(fields []uint64) (map[uint64]*dbproto.User, error) {
				return op.decode(op.rdb.Do(context.Background(), rpb.Appenduint64Args([]string{"HMGET", op.key}, fields)...))
}

func (op *DbprotoUserOP)PipelineHMGET(pipeline *rpb.Pipeline, fields []uint64, cb func(map[uint64]*dbproto.User, error)) {
				pipeline.Add(rpb.Appenduint64Args([]string{"HMGET", op.key}, fields), func(cmd *redis.Cmd){cb(op.decode(cmd))})
}

func (op *DbprotoUserOP)HEXISTS(field uint64) (bool ,error){
				result, err := op.rdb.Do(context.Background(), "HEXISTS", op.key, field).Int()
				return rpb.Ifb(err==nil, result==1, false), err
}

func (op *DbprotoUserOP)PipelineHEXISTS(pipeline *rpb.Pipeline,field uint64, cb func(bool ,error)){
				pipeline.Add([]interface{}{"HEXISTS", op.key, field}, func(cmd *redis.Cmd) {
								result, err := cmd.Int()
								cb(rpb.Ifb(err==nil, result==1, false), err)
				})
}

func (op *DbprotoUserOP)HKEYS() ([]uint64, error) {
								return op.rdb.Do(context.Background(), "HKEYS", op.key).Uint64Slice()
}

func (op *DbprotoUserOP)PipelineHKEYS(pipeline *rpb.Pipeline, cb func([]uint64, error)) {
				pipeline.Add([]interface{}{"HKEYS", op.key}, func(cmd *redis.Cmd){
								cb(cmd.Uint64Slice())
				})
}

func (op *DbprotoUserOP)HLEN() (int, error) { 
				return op.rdb.Do(context.Background(), "HLEN", op.key).Int() 
}

func (op *DbprotoUserOP)PipelineHLEN(pipeline *rpb.Pipeline, cb func(int, error)) { 
				pipeline.Add([]interface{}{"HLEN", op.key}, func(cmd *redis.Cmd){cb(cmd.Int())})
}

func (op *DbprotoUserOP)HVALS() (map[uint64]*dbproto.User, error) { return op.HGETALL() }

func (op *DbprotoUserOP)PipelineHVALS(pipeline *rpb.Pipeline, cb func(map[uint64]*dbproto.User, error)) {
				op.PipelineHGETALL(pipeline, cb)
}

func (op *DbprotoUserOP)DEL() (error) {
				return op.rdb.Do(context.Background(), "DEL", op.key).Err()
}

func (op *DbprotoUserOP)PipelineDEL(pipeline *rpb.Pipeline, cb func(error)) {
				pipeline.Add([]interface{}{ "DEL", op.key}, func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *DbprotoUserOP)HDEL(fields []uint64) (error) {
				return op.rdb.Do(context.Background(), rpb.Appenduint64Args([]string{"HDEL", op.key}, fields)...).Err()
}

func (op *DbprotoUserOP)PipelineHDEL(pipeline *rpb.Pipeline, fields []uint64, cb func(error)) {
				pipeline.Add(rpb.Appenduint64Args([]string{"HDEL", op.key}, fields), func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *DbprotoUserOP)HMSET(datas []*dbproto.User) (error) {
				args, err := op.hmsetArg(datas)
				if err != nil {
								return err
				}
				return op.rdb.Do(context.Background(), args...).Err()
}

func (op *DbprotoUserOP)PipelineHMSET(pipeline *rpb.Pipeline, datas []*dbproto.User, cb func(error)) {
				args, err := op.hmsetArg(datas)
				if err != nil {
						pipeline.SetError(fmt.Errorf("hmset dbproto.User unmarshal err:%s", err))
				}
				pipeline.Add(args, func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *DbprotoUserOP)hmsetArg(datas []*dbproto.User) ([]interface{}, error){
				args := make([]interface{}, 2, len(datas)*2+2)
				args[0], args[1] = "HMSET", op.key
				for _, data := range datas {
								value, err := proto.Marshal(data)
								if err != nil {
												return nil, err
								}
								args = append(args, data.Uid, value)
				}
				return args, nil
}


func (op* DbprotoUserOP) decode(cmd *redis.Cmd) (map[uint64]*dbproto.User, error) {
				ss, err := cmd.StringSlice()	
				if err==redis.Nil {
								return make(map[uint64]*dbproto.User), nil
				}
				if err!=nil {
								return nil, err
				}
				result := make(map[uint64]*dbproto.User, len(ss)/2)
				for i:=0;i<len(ss); i+=2 {
								msg := &dbproto.User{}
								if err := proto.Unmarshal([]byte(ss[i+1]), msg); err!=nil {
												return nil, err
								}
								result[msg.Uid] = msg
				}
				return result, nil
}


type DbprotoItemOP struct{
				rdb *redis.Client
				key string
}

func NewDbprotoItemOP(rdb *redis.Client,prefix uint64) *DbprotoItemOP {
				key := "item:"+strconv.FormatUint(uint64(prefix), 10)
				return &DbprotoItemOP{
								rdb: rdb,
								key: key,
				}
}

func (op *DbprotoItemOP)HGETALL() (map[uint32]*dbproto.Item, error) {
				return op.decode(op.rdb.Do(context.Background(), "HGETALL", op.key))
}

func (op *DbprotoItemOP)PipelineHGETALL(pipeline *rpb.Pipeline, cb func(map[uint32]*dbproto.Item, error)) {
				pipeline.Add([]interface{}{"HGETALL", op.key}, func(cmd *redis.Cmd) {cb(op.decode(cmd))})
}

func (op *DbprotoItemOP)HMGET(fields []uint32) (map[uint32]*dbproto.Item, error) {
				return op.decode(op.rdb.Do(context.Background(), rpb.Appenduint32Args([]string{"HMGET", op.key}, fields)...))
}

func (op *DbprotoItemOP)PipelineHMGET(pipeline *rpb.Pipeline, fields []uint32, cb func(map[uint32]*dbproto.Item, error)) {
				pipeline.Add(rpb.Appenduint32Args([]string{"HMGET", op.key}, fields), func(cmd *redis.Cmd){cb(op.decode(cmd))})
}

func (op *DbprotoItemOP)HEXISTS(field uint32) (bool ,error){
				result, err := op.rdb.Do(context.Background(), "HEXISTS", op.key, field).Int()
				return rpb.Ifb(err==nil, result==1, false), err
}

func (op *DbprotoItemOP)PipelineHEXISTS(pipeline *rpb.Pipeline,field uint32, cb func(bool ,error)){
				pipeline.Add([]interface{}{"HEXISTS", op.key, field}, func(cmd *redis.Cmd) {
								result, err := cmd.Int()
								cb(rpb.Ifb(err==nil, result==1, false), err)
				})
}

func (op *DbprotoItemOP)HKEYS() ([]uint32, error) {
								u64s, err := op.rdb.Do(context.Background(), "HKEYS", op.key).Uint64Slice()
								return rpb.U64ToU32s(u64s), err
}

func (op *DbprotoItemOP)PipelineHKEYS(pipeline *rpb.Pipeline, cb func([]uint32, error)) {
				pipeline.Add([]interface{}{"HKEYS", op.key}, func(cmd *redis.Cmd){
								u64s, err := op.rdb.Do(context.Background(), "HKEYS", op.key).Uint64Slice()
								cb(rpb.U64ToU32s(u64s), err)
				})
}

func (op *DbprotoItemOP)HLEN() (int, error) { 
				return op.rdb.Do(context.Background(), "HLEN", op.key).Int() 
}

func (op *DbprotoItemOP)PipelineHLEN(pipeline *rpb.Pipeline, cb func(int, error)) { 
				pipeline.Add([]interface{}{"HLEN", op.key}, func(cmd *redis.Cmd){cb(cmd.Int())})
}

func (op *DbprotoItemOP)HVALS() (map[uint32]*dbproto.Item, error) { return op.HGETALL() }

func (op *DbprotoItemOP)PipelineHVALS(pipeline *rpb.Pipeline, cb func(map[uint32]*dbproto.Item, error)) {
				op.PipelineHGETALL(pipeline, cb)
}

func (op *DbprotoItemOP)DEL() (error) {
				return op.rdb.Do(context.Background(), "DEL", op.key).Err()
}

func (op *DbprotoItemOP)PipelineDEL(pipeline *rpb.Pipeline, cb func(error)) {
				pipeline.Add([]interface{}{ "DEL", op.key}, func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *DbprotoItemOP)HDEL(fields []uint32) (error) {
				return op.rdb.Do(context.Background(), rpb.Appenduint32Args([]string{"HDEL", op.key}, fields)...).Err()
}

func (op *DbprotoItemOP)PipelineHDEL(pipeline *rpb.Pipeline, fields []uint32, cb func(error)) {
				pipeline.Add(rpb.Appenduint32Args([]string{"HDEL", op.key}, fields), func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *DbprotoItemOP)HMSET(datas []*dbproto.Item) (error) {
				args, err := op.hmsetArg(datas)
				if err != nil {
								return err
				}
				return op.rdb.Do(context.Background(), args...).Err()
}

func (op *DbprotoItemOP)PipelineHMSET(pipeline *rpb.Pipeline, datas []*dbproto.Item, cb func(error)) {
				args, err := op.hmsetArg(datas)
				if err != nil {
						pipeline.SetError(fmt.Errorf("hmset dbproto.Item unmarshal err:%s", err))
				}
				pipeline.Add(args, func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *DbprotoItemOP)hmsetArg(datas []*dbproto.Item) ([]interface{}, error){
				args := make([]interface{}, 2, len(datas)*2+2)
				args[0], args[1] = "HMSET", op.key
				for _, data := range datas {
								value, err := proto.Marshal(data)
								if err != nil {
												return nil, err
								}
								args = append(args, data.ItemId, value)
				}
				return args, nil
}


func (op* DbprotoItemOP) decode(cmd *redis.Cmd) (map[uint32]*dbproto.Item, error) {
				ss, err := cmd.StringSlice()	
				if err==redis.Nil {
								return make(map[uint32]*dbproto.Item), nil
				}
				if err!=nil {
								return nil, err
				}
				result := make(map[uint32]*dbproto.Item, len(ss)/2)
				for i:=0;i<len(ss); i+=2 {
								msg := &dbproto.Item{}
								if err := proto.Unmarshal([]byte(ss[i+1]), msg); err!=nil {
												return nil, err
								}
								result[msg.ItemId] = msg
				}
				return result, nil
}


type DbprotoOrderOP struct{
				rdb *redis.Client
				key string
}

func NewDbprotoOrderOP(rdb *redis.Client,prefix uint64) *DbprotoOrderOP {
				key := "order:"+strconv.FormatUint(uint64(prefix), 10)
				return &DbprotoOrderOP{
								rdb: rdb,
								key: key,
				}
}

func (op *DbprotoOrderOP)HGETALL() (map[string]*dbproto.Order, error) {
				return op.decode(op.rdb.Do(context.Background(), "HGETALL", op.key))
}

func (op *DbprotoOrderOP)PipelineHGETALL(pipeline *rpb.Pipeline, cb func(map[string]*dbproto.Order, error)) {
				pipeline.Add([]interface{}{"HGETALL", op.key}, func(cmd *redis.Cmd) {cb(op.decode(cmd))})
}

func (op *DbprotoOrderOP)HMGET(fields []string) (map[string]*dbproto.Order, error) {
				return op.decode(op.rdb.Do(context.Background(), rpb.AppendstringArgs([]string{"HMGET", op.key}, fields)...))
}

func (op *DbprotoOrderOP)PipelineHMGET(pipeline *rpb.Pipeline, fields []string, cb func(map[string]*dbproto.Order, error)) {
				pipeline.Add(rpb.AppendstringArgs([]string{"HMGET", op.key}, fields), func(cmd *redis.Cmd){cb(op.decode(cmd))})
}

func (op *DbprotoOrderOP)HEXISTS(field string) (bool ,error){
				result, err := op.rdb.Do(context.Background(), "HEXISTS", op.key, field).Int()
				return rpb.Ifb(err==nil, result==1, false), err
}

func (op *DbprotoOrderOP)PipelineHEXISTS(pipeline *rpb.Pipeline,field string, cb func(bool ,error)){
				pipeline.Add([]interface{}{"HEXISTS", op.key, field}, func(cmd *redis.Cmd) {
								result, err := cmd.Int()
								cb(rpb.Ifb(err==nil, result==1, false), err)
				})
}

func (op *DbprotoOrderOP)HKEYS() ([]string, error) {
								return op.rdb.Do(context.Background(), "HKEYS", op.key).StringSlice()
}

func (op *DbprotoOrderOP)PipelineHKEYS(pipeline *rpb.Pipeline, cb func([]string, error)) {
				pipeline.Add([]interface{}{"HKEYS", op.key}, func(cmd *redis.Cmd){
								cb(cmd.StringSlice())
				})
}

func (op *DbprotoOrderOP)HLEN() (int, error) { 
				return op.rdb.Do(context.Background(), "HLEN", op.key).Int() 
}

func (op *DbprotoOrderOP)PipelineHLEN(pipeline *rpb.Pipeline, cb func(int, error)) { 
				pipeline.Add([]interface{}{"HLEN", op.key}, func(cmd *redis.Cmd){cb(cmd.Int())})
}

func (op *DbprotoOrderOP)HVALS() (map[string]*dbproto.Order, error) { return op.HGETALL() }

func (op *DbprotoOrderOP)PipelineHVALS(pipeline *rpb.Pipeline, cb func(map[string]*dbproto.Order, error)) {
				op.PipelineHGETALL(pipeline, cb)
}

func (op *DbprotoOrderOP)DEL() (error) {
				return op.rdb.Do(context.Background(), "DEL", op.key).Err()
}

func (op *DbprotoOrderOP)PipelineDEL(pipeline *rpb.Pipeline, cb func(error)) {
				pipeline.Add([]interface{}{ "DEL", op.key}, func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *DbprotoOrderOP)HDEL(fields []string) (error) {
				return op.rdb.Do(context.Background(), rpb.AppendstringArgs([]string{"HDEL", op.key}, fields)...).Err()
}

func (op *DbprotoOrderOP)PipelineHDEL(pipeline *rpb.Pipeline, fields []string, cb func(error)) {
				pipeline.Add(rpb.AppendstringArgs([]string{"HDEL", op.key}, fields), func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *DbprotoOrderOP)HMSET(datas []*dbproto.Order) (error) {
				args, err := op.hmsetArg(datas)
				if err != nil {
								return err
				}
				return op.rdb.Do(context.Background(), args...).Err()
}

func (op *DbprotoOrderOP)PipelineHMSET(pipeline *rpb.Pipeline, datas []*dbproto.Order, cb func(error)) {
				args, err := op.hmsetArg(datas)
				if err != nil {
						pipeline.SetError(fmt.Errorf("hmset dbproto.Order unmarshal err:%s", err))
				}
				pipeline.Add(args, func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *DbprotoOrderOP)hmsetArg(datas []*dbproto.Order) ([]interface{}, error){
				args := make([]interface{}, 2, len(datas)*2+2)
				args[0], args[1] = "HMSET", op.key
				for _, data := range datas {
								value, err := proto.Marshal(data)
								if err != nil {
												return nil, err
								}
								args = append(args, data.OrderId, value)
				}
				return args, nil
}


func (op* DbprotoOrderOP) decode(cmd *redis.Cmd) (map[string]*dbproto.Order, error) {
				ss, err := cmd.StringSlice()	
				if err==redis.Nil {
								return make(map[string]*dbproto.Order), nil
				}
				if err!=nil {
								return nil, err
				}
				result := make(map[string]*dbproto.Order, len(ss)/2)
				for i:=0;i<len(ss); i+=2 {
								msg := &dbproto.Order{}
								if err := proto.Unmarshal([]byte(ss[i+1]), msg); err!=nil {
												return nil, err
								}
								result[msg.OrderId] = msg
				}
				return result, nil
}

