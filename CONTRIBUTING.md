# Contributing to Phonowire

Phonowire is a source-first workspace. It uses Rust 1.98.1, pinned in
`rust-toolchain.toml`; install that toolchain with its `rustfmt` and `clippy`
components before making a change:

```sh
rustup toolchain install 1.98.1 --profile minimal --component rustfmt --component clippy
```

Keep codec, receiver, and capture responsibilities separate. The codec accepts
caller-owned byte storage, the receiver owns bounded Linux TCP reception, and
the capture application owns recording output. Changes should preserve those
ownership and resource boundaries and include focused tests for observable
behavior.

Before proposing a change, run from the repository root:

```sh
cargo fmt --all -- --check
cargo build --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo test --workspace --doc --locked
```

The crates remain `publish = false`. Use local path dependencies when trying
them from another application; see the source-use example in the README.
