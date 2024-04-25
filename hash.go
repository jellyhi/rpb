package rpb

import (
	"io"
	"reflect"
	"strings"
	"text/template"

	"github.com/gogo/protobuf/proto"
)

const hashTpl = `
type {{.ProtoPkgCamel}}{{.ProtoTypeName}}OP struct{
				rdb *redis.Client
				key string
}

func New{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP(rdb *redis.Client{{if .HasPrefix}},prefix {{.PrefixType}}{{end}}) *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP {
				{{- if .HasPrefix}}
								{{- if eq .PrefixType "string"}}
				key := "{{.RedisKeyName}}"+prefix
								{{- else}}
				key := "{{.RedisKeyName}}"+strconv.FormatUint(uint64(prefix), 10)
								{{- end}}
				{{- else}}
				key := "{{.RedisKeyName}}"
				{{- end}}
				return &{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP{
								rdb: rdb,
								key: key,
				}
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)HGETALL() (map[{{.ProtoKeyTypeName}}]*{{.ProtoPkg}}.{{.ProtoTypeName}}, error) {
				return op.decode(op.rdb.Do(context.Background(), "HGETALL", op.key))
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)PipelineHGETALL(pipeline *rpb.Pipeline, cb func(map[{{.ProtoKeyTypeName}}]*{{.ProtoPkg}}.{{.ProtoTypeName}}, error)) {
				pipeline.Add([]interface{}{"HGETALL", op.key}, func(cmd *redis.Cmd) {cb(op.decode(cmd))})
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)HMGET(fields []{{.ProtoKeyTypeName}}) (map[{{.ProtoKeyTypeName}}]*{{.ProtoPkg}}.{{.ProtoTypeName}}, error) {
				return op.decode(op.rdb.Do(context.Background(), rpb.Append{{.ProtoKeyTypeName}}Args([]string{"HMGET", op.key}, fields)...))
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)PipelineHMGET(pipeline *rpb.Pipeline, fields []{{.ProtoKeyTypeName}}, cb func(map[{{.ProtoKeyTypeName}}]*{{.ProtoPkg}}.{{.ProtoTypeName}}, error)) {
				pipeline.Add(rpb.Append{{.ProtoKeyTypeName}}Args([]string{"HMGET", op.key}, fields), func(cmd *redis.Cmd){cb(op.decode(cmd))})
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)HEXISTS(field {{.ProtoKeyTypeName}}) (bool ,error){
				result, err := op.rdb.Do(context.Background(), "HEXISTS", op.key, field).Int()
				return rpb.Ifb(err==nil, result==1, false), err
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)PipelineHEXISTS(pipeline *rpb.Pipeline,field {{.ProtoKeyTypeName}}, cb func(bool ,error)){
				pipeline.Add([]interface{}{"HEXISTS", op.key, field}, func(cmd *redis.Cmd) {
								result, err := cmd.Int()
								cb(rpb.Ifb(err==nil, result==1, false), err)
				})
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)HKEYS() ([]{{.ProtoKeyTypeName}}, error) {
				{{- if eq .ProtoKeyTypeName "string"}}
								return op.rdb.Do(context.Background(), "HKEYS", op.key).StringSlice()
				{{- else if eq .ProtoKeyTypeName "uint64"}}
								return op.rdb.Do(context.Background(), "HKEYS", op.key).Uint64Slice()
				{{- else if eq .ProtoKeyTypeName "uint32"}}
								u64s, err := op.rdb.Do(context.Background(), "HKEYS", op.key).Uint64Slice()
								return rpb.U64ToU32s(u64s), err
				{{- else}}
								return nil, errors.New("unsupported key type")
				{{- end}}
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)PipelineHKEYS(pipeline *rpb.Pipeline, cb func([]{{.ProtoKeyTypeName}}, error)) {
				pipeline.Add([]interface{}{"HKEYS", op.key}, func(cmd *redis.Cmd){
				{{- if eq .ProtoKeyTypeName "string"}}
								cb(cmd.StringSlice())
				{{- else if eq .ProtoKeyTypeName "uint64"}}
								cb(cmd.Uint64Slice())
				{{- else if eq .ProtoKeyTypeName "uint32"}}
								u64s, err := op.rdb.Do(context.Background(), "HKEYS", op.key).Uint64Slice()
								cb(rpb.U64ToU32s(u64s), err)
				{{- else}}
								cb(nil, errors.New("unsupported key type"))
				{{- end}}
				})
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)HLEN() (int, error) { 
				return op.rdb.Do(context.Background(), "HLEN", op.key).Int() 
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)PipelineHLEN(pipeline *rpb.Pipeline, cb func(int, error)) { 
				pipeline.Add([]interface{}{"HLEN", op.key}, func(cmd *redis.Cmd){cb(cmd.Int())})
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)HVALS() (map[{{.ProtoKeyTypeName}}]*{{.ProtoPkg}}.{{.ProtoTypeName}}, error) { return op.HGETALL() }

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)PipelineHVALS(pipeline *rpb.Pipeline, cb func(map[{{.ProtoKeyTypeName}}]*{{.ProtoPkg}}.{{.ProtoTypeName}}, error)) {
				op.PipelineHGETALL(pipeline, cb)
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)DEL() (error) {
				return op.rdb.Do(context.Background(), "DEL", op.key).Err()
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)PipelineDEL(pipeline *rpb.Pipeline, cb func(error)) {
				pipeline.Add([]interface{}{ "DEL", op.key}, func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)HDEL(fields []{{.ProtoKeyTypeName}}) (error) {
				return op.rdb.Do(context.Background(), rpb.Append{{.ProtoKeyTypeName}}Args([]string{"HDEL", op.key}, fields)...).Err()
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)PipelineHDEL(pipeline *rpb.Pipeline, fields []{{.ProtoKeyTypeName}}, cb func(error)) {
				pipeline.Add(rpb.Append{{.ProtoKeyTypeName}}Args([]string{"HDEL", op.key}, fields), func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)HMSET(datas []*{{.ProtoPkg}}.{{.ProtoTypeName}}) (error) {
				args, err := op.hmsetArg(datas)
				if err != nil {
								return err
				}
				return op.rdb.Do(context.Background(), args...).Err()
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)PipelineHMSET(pipeline *rpb.Pipeline, datas []*{{.ProtoPkg}}.{{.ProtoTypeName}}, cb func(error)) {
				args, err := op.hmsetArg(datas)
				if err != nil {
						pipeline.SetError(fmt.Errorf("hmset {{.ProtoPkg}}.{{.ProtoTypeName}} unmarshal err:%s", err))
				}
				pipeline.Add(args, func(cmd *redis.Cmd){cb(cmd.Err())})
}

func (op *{{.ProtoPkgCamel}}{{.ProtoTypeName}}OP)hmsetArg(datas []*{{.ProtoPkg}}.{{.ProtoTypeName}}) ([]interface{}, error){
				args := make([]interface{}, 2, len(datas)*2+2)
				args[0], args[1] = "HMSET", op.key
				for _, data := range datas {
								value, err := proto.Marshal(data)
								if err != nil {
												return nil, err
								}
								args = append(args, data.{{.ProtoKeyFieldName}}, value)
				}
				return args, nil
}


func (op* {{.ProtoPkgCamel}}{{.ProtoTypeName}}OP) decode(cmd *redis.Cmd) (map[{{.ProtoKeyTypeName}}]*{{.ProtoPkg}}.{{.ProtoTypeName}}, error) {
				ss, err := cmd.StringSlice()	
				if err==redis.Nil {
								return make(map[{{.ProtoKeyTypeName}}]*{{.ProtoPkg}}.{{.ProtoTypeName}}), nil
				}
				if err!=nil {
								return nil, err
				}
				result := make(map[{{.ProtoKeyTypeName}}]*{{.ProtoPkg}}.{{.ProtoTypeName}}, len(ss)/2)
				for i:=0;i<len(ss); i+=2 {
								msg := &{{.ProtoPkg}}.{{.ProtoTypeName}}{}
								if err := proto.Unmarshal([]byte(ss[i+1]), msg); err!=nil {
												return nil, err
								}
								result[msg.{{.ProtoKeyFieldName}}] = msg
				}
				return result, nil
}

`

type hashParam struct {
	ProtoPkg          string
	ProtoPkgCamel     string
	ProtoTypeName     string
	ProtoKeyTypeName  string
	ProtoKeyFieldName string

	RedisKeyName string
	PrefixType   PrefixType
	HasPrefix    bool
}

var hashParams []*hashParam

func RegisterHash(keyName string, prefixTyp PrefixType, message proto.Message) {
	typ := reflect.TypeOf(message)
	if keyName == "" {
		panicf("keyName is empty")
	}
	if keyName[len(keyName)-1] == ':' {
		if prefixTyp == PrefixNone {
			panicf("keyName %s miss prefix type", keyName)
		}
	} else {
		if prefixTyp != PrefixNone {
			panicf("keyName %s has prefix type", keyName)
		}
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	param := &hashParam{
		RedisKeyName: keyName,
		PrefixType:   prefixTyp,
		HasPrefix:    prefixTyp != PrefixNone,
	}
	//hasPrefix := keyName[len(keyName)-1] == ':'
	var firstField *reflect.StructField
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		//第一个field作为redis hash key的field，必须是string/int32/int64/uint32/uint64类型
		if firstField == nil {
			firstField = &field
			switch field.Type.Kind() {
			case reflect.String, reflect.Int32, reflect.Int64, reflect.Uint32, reflect.Uint64:
			default:
				panicf("first field %s of type %s is not a supported type", field.Name, field.Type.Name())
			}
		}
	}
	if firstField == nil {
		panicf("type %s has no field", typ.Name())
	}
	param.ProtoPkg = typ.PkgPath()
	if idx := strings.LastIndex(param.ProtoPkg, "/"); idx != -1 {
		param.ProtoPkg = param.ProtoPkg[idx+1:]
	}
	param.ProtoPkgCamel = toCamel(param.ProtoPkg)
	param.ProtoTypeName = typ.Name()
	param.ProtoKeyTypeName = firstField.Type.Name()
	param.ProtoKeyFieldName = firstField.Name
	param.PrefixType = prefixTyp
	param.HasPrefix = prefixTyp != PrefixNone
	hashParams = append(hashParams, param)

	headerParam.Imports = append(headerParam.Imports, typ.PkgPath())
	headerParam.KeyMetas = append(headerParam.KeyMetas, &KeyMeta{
		KeyName:      keyName,
		KeyType:      KeyTypeHash,
		HasPrefix:    param.HasPrefix,
		ProtoMessage: param.ProtoPkg + "." + param.ProtoTypeName,
	})
}

func GenerateCode(keyNamespace, pkg string, writer io.Writer) {
	headerParam.Pkg = pkg
	headerParam.KeyMetaNamespace = keyNamespace
	headerParam.Imports = uniqueStringSlice(headerParam.Imports)
	tpl := template.Must(template.New("header").Parse(headerTpl))
	tpl.Execute(writer, headerParam)

	tpl2 := template.Must(template.New("hash").Parse(hashTpl))
	for _, param := range hashParams {
		tpl2.Execute(writer, param)
	}
}
