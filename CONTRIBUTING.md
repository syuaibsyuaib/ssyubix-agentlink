# Contributing to agentlink-ssyubix

Thanks for helping improve `agentlink-ssyubix`.

## Before You Start

- Open an issue for major features, protocol changes, or breaking API changes
- Keep pull requests focused and easy to review
- Update documentation when behavior or setup changes
- Add changelog notes for user-visible changes

## Repository Layout

- `src/` contains the Cloudflare Worker and Durable Object backend
- `python/` contains the Python MCP package

## Local Setup

Python package:

```bash
cd python
python -m pip install --upgrade pip
python -m pip install -e .
```

Run the Python tests:

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

Build the Python package:

```bash
python -m build
```

Validate the Cloudflare Worker bundle:

```bash
cd ..
npx wrangler deploy --config src/wrangler.jsonc --dry-run
```

## Release Process

1. Update `python/pyproject.toml` with the new version
2. Add release notes to `CHANGELOG.md`
3. Commit the changes and push to `main`
4. Create and push a Git tag like `v2.0.1`
5. Let `.github/workflows/release.yml` publish to PyPI via Trusted Publishing

Before the first automated release, configure the PyPI Trusted Publisher for:

- owner: `syuaibsyuaib`
- repository: `agentlink-ssyubix`
- workflow file: `.github/workflows/release.yml`
- environment: `pypi`

## Pull Request Checklist

- Add or update tests when behavior changes
- Keep changes backwards compatible unless the PR is clearly marked as breaking
- Explain any protocol or deployment impact in the PR description
- Avoid committing local caches, build outputs, or credentials
