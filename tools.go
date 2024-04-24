package rpb

import (
	"fmt"
	"strings"
)

func panicf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func toCamel(underscore string) string {
	parts := strings.Split(underscore, "_")
	var result string
	for _, part := range parts {
		result += strings.Title(part)
	}
	return result
}

type PrefixType string

const PrefixNone PrefixType = ""
const PrefixUint32 PrefixType = "uint32"
const PrefixUint64 PrefixType = "uint64"
const PrefixString PrefixType = "string"

func Appenduint64Args(ss []string, vv []uint64) []interface{} {
	result := make([]interface{}, 0, len(ss)+len(vv))
	for _, s := range ss {
		result = append(result, s)
	}
	for _, v := range vv {
		result = append(result, v)
	}
	return result
}

func Appenduint32Args(ss []string, vv []uint32) []interface{} {
	result := make([]interface{}, 0, len(ss)+len(vv))
	for _, s := range ss {
		result = append(result, s)
	}
	for _, v := range vv {
		result = append(result, v)
	}
	return result
}

func U64ToU32s(vv []uint64) []uint32 {
	result := make([]uint32, len(vv))
	for i, v := range vv {
		result[i] = uint32(v)
	}
	return result
}

func Ifb(ok, okret, failret bool) bool {
	if ok {
		return okret
	}
	return failret
}
