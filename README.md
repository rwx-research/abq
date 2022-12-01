# abq

## Development

Install [rustup](https://rustup.rs); local versions of Rust will be populated
when you run `cargo` in this project.

Then install cargo-insta for testing

```bash
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

### Dev queues

[Automated tests](.github/workflows/bigtest.yml) are run against remote instances of
feature-branch queues when you submit a PR. These feature-branch queues are
called "dev queues" and have `-devel` version suffixes, e.g. `1.0.0-15-gd923df5-devel`.

Sometimes, you may want to perform local manual testing against a remote dev
queue. To aid this, use `bin/manage_dev_queue`.

To get started, you need to have permission to create queues manually on Captain
staging. This script expects `ABQ_CREATE_MANUAL_ACCESS_TOKEN` to be available in
your environment with the access token, which you can get via

```bash
aws sso login --profile staging
aws ssm get-parameter --name /captain_staging/env/ABQ_CREATE_MANUAL_ACCESS_TOKEN --with-decryption --profile staging
```

or if you're using direnv, write ABQ_CREATE_MANUAL_ACCESS to your .envrc

```bash
aws ssm get-parameter --name /captain_staging/env/ABQ_CREATE_MANUAL_ACCESS_TOKEN --with-decryption --profile staging --output json | jq .Parameter.Value | xargs -n 1 -I {} echo "ABQ_CREATE_MANUAL_ACCESS_TOKEN={}" > .envrc
```

- `bin/manage_dev_queue start <version>` - start the dev queue instance. If no
  version is provided, the head commit version will be used. You should make
  sure that the version has been [built and published](.github/workflows/test_and_package_development.yml)
  to Captain/ABQ staging for this to work.
  - Only one dev queue can be active at a time.
- `bin/manage_dev_queue stop` - stops the active dev queue instance, if any.

## Operations

### Releasing a new version

1. create a new branch
2. bump the version in crates/abq_cli/Cargo.toml
3. run `cargo b` to update lockfiles
4. merge the branch. MERGE BEFORE YOU TAG! (squash commit will leave any pre-merge tag on a branch dangling!)
5. push a signed tag against the merge commit in master, e.g. `git tag v1.2.3 -s -m "version message" && git push --tags`
6. [run the build and upload workflow](https://github.com/rwx-research/abq/actions/workflows/build_and_upload.yml) with your new tag as the ref

### SSL Certificates

When TLS is enabled, clients are designed to communicate with a queue instance
with SAN DNS name `abq.rwx` and a self-signed certificate that is passed to the
client at runtime.

An example of how to create such a certificate follows:

```bash
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
