# Coding Agents

## Claude Code

```bash
curl -fsSL https://claude.ai/install.sh | bash
```

## OpenAI Codex

Codex requires Node.js/npm. Install both without sudo using nvm (Node Version Manager), which installs everything into your home directory.

### 1. Install nvm

```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
```

Reload your shell:

```bash
source ~/.bashrc   # or ~/.zshrc if you use zsh
```

### 2. Install Node.js (includes npm)

```bash
nvm install --lts
nvm use --lts
```

Verify:

```bash
node -v
npm -v
which npm    # should show something like ~/.nvm/versions/node/...
```

### 3. Install Codex

```bash
npm install -g @openai/codex
```
