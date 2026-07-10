# Contributing

Thanks for your interest!

- **Report a bug** -- open an issue
- **Fix a bug** -- fork, branch, PR
- **Suggest a feature** -- open an issue first

## Development

```bash
# Clone both repos side-by-side (autorip depends on ../libfreemkv)
git clone https://github.com/freemkv/libfreemkv
git clone https://github.com/freemkv/autorip
cd autorip
cargo build
cargo test
```

## License

MIT

## Release Process

Release triggers Docker image build to GHCR. Requires a semver tag:

```bash
git tag -a v1.0.0-rc.1 -m "v1.0.0-rc.1"
git push origin v1.0.0-rc.1
```

The Release workflow builds and pushes:
- `ghcr.io/freemkv/autorip:latest`
- `ghcr.io/freemkv/autorip:1.0.0-rc.1`
