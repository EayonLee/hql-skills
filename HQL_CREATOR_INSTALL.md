# HQL Creator 安装说明

这份文档只负责安装，不负责使用方法。  
使用方法见 [SKILL.md](./SKILL.md)。

## 重要提醒

skill 复制完成后，**必须重启对应的 agent**，新的 skill 才会被重新扫描和加载。  
如果不重启，即使目录和依赖都已经准备好，agent 也可能仍然看不到这个 skill。

## 1. 安装 skill 本体

先确认你当前使用的是哪一个 agent，然后只执行对应小节。

把 `hql-creator` 目录复制或链接到对应的 skills 目录中。

### Codex

```bash
mkdir -p ~/.codex/skills
cp -R hql-creator ~/.codex/skills/
```

### Claude Code

```bash
mkdir -p ~/.claude/skills
cp -R hql-creator ~/.claude/skills/
```

### OpenCode

```bash
mkdir -p ~/.config/opencode/skills
cp -R hql-creator ~/.config/opencode/skills/
```

### OpenClaw

推荐安装到共享 skills 目录：

```bash
mkdir -p ~/.openclaw/skills
cp -R hql-creator ~/.openclaw/skills/
```

如果你只想让当前 workspace 使用这个 skill，也可以安装到：

```bash
mkdir -p ~/.openclaw/workspace/skills
cp -R hql-creator ~/.openclaw/workspace/skills/
```

### Hermes

```bash
mkdir -p ~/.hermes/skills
cp -R hql-creator ~/.hermes/skills/
```

如果你更喜欢符号链接，也可以用 `ln -s` 代替 `cp -R`。

## 2. 安装 Python 依赖

进入你刚才安装好的 skill 目录，安装 [requirements.txt](./requirements.txt) 中的依赖。

不同 agent 下的 skill 目录分别是：

- Codex: `~/.codex/skills/hql-creator`
- Claude Code: `~/.claude/skills/hql-creator`
- OpenCode: `~/.config/opencode/skills/hql-creator`
- OpenClaw: `~/.openclaw/skills/hql-creator`
- OpenClaw workspace: `~/.openclaw/workspace/skills/hql-creator`
- Hermes: `~/.hermes/skills/hql-creator`

### 直接安装到当前 Python 环境

```bash
cd <skill-dir>
python3 -m pip install -r requirements.txt
```

### 推荐：使用虚拟环境

```bash
cd <skill-dir>
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

当前依赖：

- `pydantic`
- `rapidfuzz`
- `dateparser`

## 3. 验证安装

安装完成后，可以运行下面的命令做一次快速验证：

```bash
cd <skill-dir>
python3 -m py_compile scripts/*.py scripts/engine/*.py
python3 scripts/main.py skeleton detail
```

如果两条命令都成功，说明 skill 和依赖已经准备好。

## 4. 重新加载 agent

如果这是新安装的 skill，下面这一步是**必须执行**的：

重启对应的 agent，让它重新扫描并加载 skill。

- Codex：重启 Codex
- Claude Code：重启 Claude Code
- OpenCode：重启 OpenCode
- OpenClaw / 小龙虾：重启 OpenClaw
- Hermes：重启 Hermes
