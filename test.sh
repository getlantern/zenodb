#! /usr/bin/env sh
find . -type d | grep -v vendor | grep -v ".git" | xargs -I{} echo "(cd {}; go test -race; cd -) &&" | paste -s -d" " - | sed 's/$/ true/' | /usr/bin/env sh
