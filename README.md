# `streamdeck`

## Setup
### Install base requirements
```
brew install hidapi
pip install uv
```

### Set up Python environment
```
uv sync
```

## Test
Turn your speakers on.

```
uv run test.py
```

As prompted, press a button on the Streamdeck you're using to acquire it.

Press buttons! Try double-pressing, or press-and-hold.