#! /bin/bash

set -e

go test -v -coverprofile cover.out $1 \
&& go tool cover -func cover.out \
| grep --color=always -E '[[:blank:]][[:digit:]]?[[:digit:]]\.[[:digit:]]%$' \
|| :
