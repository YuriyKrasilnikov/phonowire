#!/bin/sh
set -eu
image_name=${1:-phonowire-capture:local}
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
copy_file() {
    source=$1
    destination="$context/${source#"$root/"}"
    test ! -L "$source" || { echo "refusing symlink input: $source" >&2; exit 1; }
    mkdir -p "$(dirname "$destination")"
    cp "$source" "$destination"
}
copy_tree() {
    source=$1
    test ! -L "$source" || { echo "refusing symlink input: $source" >&2; exit 1; }
    find "$source" -type l -print -quit | grep -q . && {
        echo "refusing symlink below allowlisted input: $source" >&2; exit 1;
    }
    find "$source" -type f -print | while IFS= read -r file; do copy_file "$file"; done
}
for file in Cargo.toml Cargo.lock rust-toolchain.toml LICENSE NOTICE \
    apps/phonowire-capture/Cargo.toml apps/phonowire-capture/Dockerfile; do
    copy_file "$root/$file"
done
copy_tree "$root/apps/phonowire-capture/src"
copy_tree "$root/crates/phonowire-audiosocket/src"
copy_tree "$root/crates/phonowire-receiver/src"
copy_file "$root/crates/phonowire-audiosocket/Cargo.toml"
copy_file "$root/crates/phonowire-receiver/Cargo.toml"
docker build -f "$context/apps/phonowire-capture/Dockerfile" -t "$image_name" "$context"
