# Contributing to graphed-executors

Bug reports, fixes, new backend adapters, and doc improvements are all welcome. This page
gets you from a clone to a green local run of everything CI checks.

## Set up a development environment

You need Python ≥ 3.11 and a Rust toolchain (`rustup` — https://rustup.rs). The `graphed`
dependency is installed from git and compiles a Rust extension during install; without a
toolchain the first `pip install` below fails.

```bash
git clone https://github.com/graphed-org/graphed-executors
cd graphed-executors
python -m venv .venv && source .venv/bin/activate

pip install "graphed[awkward,numpy] @ git+https://github.com/graphed-org/graphed@main"
pip install "graphed-corpus @ git+https://github.com/graphed-org/graphed-corpus@main"
pip install "graphed-histogram @ git+https://github.com/graphed-org/graphed-histogram@main"
pip install -e ".[dev,docs]"
```

Install `graphed-histogram` from git **before** `pip install -e ".[dev]"`, as shown. The
`dev` extra names it, and if git hasn't already satisfied it pip resolves the PyPI release
instead — everything imports, and the histogram tests exercise a different package than the
one CI pins.

Working on a cluster backend? Add its extra:

```bash
pip install -e ".[dev,dask]"  && pip install pyarrow pandas   # dask backend
pip install -e ".[dev,parsl]" && pip install pyarrow          # parsl backend
```

## Run the tests

```bash
pytest
```

Tests for the dask and parsl backends skip automatically when the extra isn't installed,
so a plain `pytest` is green in any of the environments above. CI runs the suite on Linux
(x86_64 and arm64), macOS, and Windows across all supported Python versions, with the dask
and parsl suites in dedicated jobs.

To see the coverage CI will enforce (90% line + branch on `graphed_executors`):

```bash
pytest --cov=graphed_executors --cov-branch --cov-report=term-missing
```

## Lint, types, docs

```bash
ruff check . && ruff format --check .
mypy
sphinx-build -W -b html docs docs/_build/html
```

Or run the lint and type hooks exactly as CI does, in one shot:

```bash
uvx prek run --all-files
```

`prek install` sets the same checks up as a git pre-commit hook.

## Propose a change

1. Branch from `main` and keep the change focused — one behavior per PR.
2. Cover new behavior with tests. Don't edit existing tests to make a change pass; add new
   ones (under `tests/extra/` for backend-specific additions) and explain the behavior
   change in the PR description.
3. Make sure `pytest`, `ruff`, `mypy`, and the `sphinx-build -W` docs build are clean
   locally — they are exactly what CI gates on.
4. Open the PR against `main`. CI must be green before review.
