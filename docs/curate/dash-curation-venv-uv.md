# dash_curation — venv Setup with UV

## 1. Install UV

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Reload your shell so `uv` is on your PATH:

```bash
source ~/.bashrc
```

Verify:

```bash
uv --version
```

## 2. Create the Virtual Environment

Navigate to the app directory:

```bash
cd ~/dash_curation/app
```

Create the venv:

```bash
uv venv
```

This creates a `.venv/` folder inside `~/dash_curation/app/`.

## 3. Activate and Install Dependencies

```bash
source .venv/bin/activate
uv pip install -r requirements.txt
```

## 4. Add a Bashrc Alias

Add this to `~/.bashrc` so you can activate the environment in one command:

```bash
alias dash='cd ~/dash_curation/app && source .venv/bin/activate'
```

Reload:

```bash
source ~/.bashrc
```

Then just run:

```bash
dash
```
