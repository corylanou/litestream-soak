#!/bin/bash
set -euo pipefail
source_dir=${1:?fresh source directory required}
binary=${2:?absolute output binary required}
[[ "$binary" = /* ]] || exit 2
source_sha=203d7369ecb26c9adecadb501cd95682decdb527
version=0.4.101-soak.1
git clone https://github.com/superfly/flyctl.git "$source_dir"
git -C "$source_dir" checkout --detach "$source_sha"
test "$(git -C "$source_dir" rev-parse HEAD)" = "$source_sha"
cd "$source_dir"
go version
go mod edit -require=golang.org/x/crypto@v0.56.0
go mod download golang.org/x/crypto
go mod verify
git diff -- go.mod go.sum >"${binary}.dependency.patch"
git rev-parse HEAD >"${binary}.source.sha"
cp go.mod "${binary}.go.mod"
cp go.sum "${binary}.go.sum"
go list -m -json all >"${binary}.modules.json"
CGO_ENABLED=0 go build -mod=readonly -tags production \
    -ldflags "-X github.com/superfly/flyctl/internal/buildinfo.buildVersion=${version} -X github.com/superfly/flyctl/internal/buildinfo.commit=${source_sha}+soak.1 -X github.com/superfly/flyctl/internal/buildinfo.buildDate=$(git show -s --format=%cI HEAD)" \
    -o "$binary" .
go version -m "$binary" >"${binary}.buildinfo"
"$binary" version
"$binary" logs --help | awk '/--no-tail/ {found=1; print} END {exit !found}'
"$binary" deploy --help | awk '/--remote-only/ {found=1; print} END {exit !found}'
"$binary" deploy --help | awk '/--build-only/ {found=1; print} END {exit !found}'
"$binary" deploy --help | awk '/--image-label/ {found=1; print} END {exit !found}'
"$binary" machine list --help | awk '/--json/ {found=1; print} END {exit !found}'
