# abq

## Development

Install [rustup](https://rustup.rs); local versions of Rust will be populated
when you run `cargo` in this project.

Then install cargo-insta for testing

```
cargo install cargo-insta
```

For development in an editor, you'll likely want a version of
[rust-analyzer](https://rust-analyzer.github.io/manual.html#installation).

Tips:

- If you see `Blocking waiting for file lock on build directory` on a fresh
  invocation of `cargo`, and there are no other `cargo` processes running
  against the project, it may be `rust-analyzer` taking control of the build
  cache. The easiest way to resolve this is to repoint rust-analyzer's build
  directory; I have the following config value set in my editor:

  ```json
  "rust-analyzer.checkOnSave.extraArgs": ["--target-dir", "/tmp/rust-analyzer-check"],
  ```

## Operations

### Releasing a new version

(TODO: automate me)

1. create a new branch
2. bump the version in crates/abq_cli/Cargo.toml
3. run `cargo b` to update lockfiles
4. merge the branch
5. push a signed tag against the merge commit in master, e.g. `git tag v1.2.3 -s -m "version message"`

### SSL Certificates

When TLS is enabled, clients are designed to communicate with a queue instance
with SAN DNS name `abq.rwx` and a self-signed certificate that is passed to the
client at runtime.

An example of how to create such a certificate follows:

```
openssl \
  req -x509 -newkey rsa:4096 \
  -keyout server.key -out ssl_certs/server.crt \
  -days 365 -sha256 -nodes \
  -subj '/CN=abq.rwx' \
  -extensions san \
  -config <( \
    echo '[req]'; \
    echo 'distinguished_name=req'; \
    echo '[san]'; \
    echo 'subjectAltName=DNS:abq.rwx' )
```
