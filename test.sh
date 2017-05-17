#! /usr/bin/env sh
echo "mode: count" > profile.cov
TP=$(find . -name "*_test.go" -printf '%h\n' | grep  -v vendor | grep -v glide | sort -u)
set -x && \
for pkg in $TP; do \
	go test -v -tags="headless" -covermode=atomic -coverprofile=profile_tmp.cov $pkg || exit 1; \
	tail -n +2 profile_tmp.cov >> profile.cov; \
done
exit $?
