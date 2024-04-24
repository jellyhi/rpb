package rpb

const headerTpl = `
package {{.Pkg}}

import(
				"fmt"
				"context"
				"strconv"

				{{range .Imports}}"{{.}}"
				{{end}}

				"github.com/jellyhi/rpb"
				"github.com/gogo/protobuf/proto"
				"github.com/go-redis/redis/v8"
)

func init() {
				{{- range .KeyMetas}}
				rpb.RegisterKeyMeta("{{$.KeyMetaNamespace}}", &rpb.KeyMeta{KeyType: rpb.KeyType("{{.KeyType}}"), KeyName:"{{.KeyName}}", HasPrefix: {{.HasPrefix}},ProtoMessage: "{{.ProtoMessage}}"})
				{{- end}}
}

`

var headerParam struct {
	Pkg              string
	Imports          []string
	KeyMetaNamespace string
	KeyMetas         []*KeyMeta
}
