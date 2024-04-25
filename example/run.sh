#!/bin/sh

go run genrpb/main.go && gofmt -w main/gen_rpb.go
cd main && go build && ./main && cd -
