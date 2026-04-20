# HQL-SKILLS

<div align="center">
  <img src="./assets/hql-creator-icon.png" width="300" alt="HQL-SKILLS Logo" />
  <h1 style="margin-top: 6px;">HQL-SKILLS</h1>
  <p><strong>🧠 面向 AI Agent 的 HQL 生成与查询 Skill 集合</strong></p>
  <p>
    当前仓库按两个方向拆分：
    <code>hql-creator</code> 负责自然语言转 HQL，
    <code>hql-query</code> 负责把 HQL 发送到实际服务并做结果格式化。
  </p>
  <p>
    <img alt="README Chinese" src="https://img.shields.io/badge/README-%E4%B8%AD%E6%96%87-0f766e?style=flat-square" />
    <img alt="Project" src="https://img.shields.io/badge/Project-HQL--SKILLS-0f172a?style=flat-square" />
    <img alt="Skill Count" src="https://img.shields.io/badge/Skills-2-2563eb?style=flat-square" />
    <img alt="Python" src="https://img.shields.io/badge/Python-3.9%2B-1d4ed8?style=flat-square&logo=python&logoColor=white" />
    <img alt="Agents" src="https://img.shields.io/badge/Agents-5-7c3aed?style=flat-square" />
    <img alt="Status" src="https://img.shields.io/badge/Status-Active%20Build-f59e0b?style=flat-square" />
  </p>
  <p>
    <a href="#-项目概览">项目概览</a>
    ·
    <a href="#-两个-skill">两个 Skill</a>
    ·
    <a href="#-当前已说明安装的-agent">Agent 支持</a>
    ·
    <a href="#-安装">安装</a>
    ·
    <a href="#-语法支持矩阵hql-creator">语法支持矩阵</a>
  </p>
</div>

> 📌 这个仓库的目标不是“让模型随手猜一条 HQL”，而是把 HQL 相关能力拆成更稳定、可复用、可演进的 skill 模块。

---

## 🧩 两个 Skill

| Skill         | 核心作用                           | 当前仓库状态                          | 适用场景                  |
|---------------|--------------------------------|---------------------------------|-----------------------|
| `hql-creator` | 将用户自然语言转换成可直接用于查询的 HQL 语句      | ✅ 已有 `SKILL.md`、脚本、引用文档与安装说明    | 自然语言转 HQL、字段检索、HQL 复审 |
| `hql-query`   | 将 HQL 语句发送给实际服务进行查询，并对结果做输出格式化 | 🟡 目录已预留，定位已明确，但当前仓库内尚未补齐完整实现文件 | HQL 执行、结果查询、结果格式化与展示  |

## 🎯 能力分工

| 维度    | `hql-creator`     | `hql-query`    |
|-------|-------------------|----------------|
| 输入    | 用户自然语言、查询意图、结构化请求 | 已生成的 HQL 语句    |
| 输出    | 可直接执行的 HQL        | 查询结果、格式化后的输出   |
| 主要职责  | 生成查询语句            | 发送查询、拿回结果、整理结果 |
| 关注重点  | 正确表达用户意图          | 正确执行查询并返回易读结果  |
| 当前成熟度 | 较高                | 方向明确，待补实现      |

---

## 🤖 已支持的 Agent CLI

| Agent       | 当前是否有安装说明 | 安装路径类型                                                  | 备注                     |
|-------------|-----------|---------------------------------------------------------|------------------------|
| Codex       | ✅         | `~/.codex/skills/`                                      | 当前文档已覆盖                |
| Claude Code | ✅         | `~/.claude/skills/`                                     | 当前文档已覆盖                |
| OpenCode    | ✅         | `~/.config/opencode/skills/`                            | 当前文档已覆盖                |
| OpenClaw    | ✅         | `~/.openclaw/skills/` / `~/.openclaw/workspace/skills/` | 同时支持共享安装和 workspace 安装 |
| Hermes      | ✅         | `~/.hermes/skills/`                                     | 当前文档已覆盖                |


---

## 📦 安装

### 🤖 给 LLM Agent 看的

把下面这段话发给你的 LLM Agent，让它帮你完成安装：

```text
Install and configure hql-skills by following the instructions here:
https://github.com/EayonLee/hql-skills/blob/main/installation.md
```

---

## 🚀 快速开始：从自然语言到 HQL

下面这段演示基于一次真实的输入输出过程做了精简，去掉了中间无效试错，只保留最关键的步骤。

### 📝 用户输入

```text
生成HQL：查询最近30天内被模型研判过的，并且攻击成功的告警有多少条，根据攻击地址分类，并统计每个攻击地址的数量
```

### ✅ 输出结果

```sql
index == "alarm_merge" | where 安全大模型研判 == "攻击成功" and 开始时间 >= now(d-30d) and 开始时间 <= now() | stats count(ID) AS 数量 BY 源地址 | sort -数量
```

### 🔍 这个演示体现了什么

| 观察点 | 说明 |
| --- | --- |
| 自然语言转 HQL | 用户没有直接写 HQL，skill 最终生成了可执行查询语句 |
| 语义条件处理 | “被模型研判过”“攻击成功” 被正确落入语义层条件 |
| 时间条件处理 | “最近30天” 被转换为 `now(d-30d)` 到 `now()` |
| 分组统计处理 | “根据攻击地址分类，并统计数量” 被转换成 `stats count(ID) AS 数量 BY ...` |
| 字段绑定 | 请求中的“攻击地址”最终绑定为查询字段 `源地址`，这是当前 skill 的字段映射行为之一 |

## 🐛 DEBUG

### `hql-creator` 常用命令

| 目标          | 命令                                                                     |
|-------------|------------------------------------------------------------------------|
| 查看明细骨架      | `cd hql-creator && python3 scripts/main.py skeleton detail`            |
| 查看总数骨架      | `cd hql-creator && python3 scripts/main.py skeleton aggregate-total`   |
| 查看分组骨架      | `cd hql-creator && python3 scripts/main.py skeleton aggregate-grouped` |
| 查看 Top-K 骨架 | `cd hql-creator && python3 scripts/main.py skeleton aggregate-topk`    |
| 复审现有 HQL    | `cd hql-creator && python3 scripts/main.py review 告警 '<现成HQL>'`        |
| 检索字段        | `cd hql-creator && python3 scripts/main.py lookup-fields 告警 攻击地址`      |

### `hql-creator` 输入输出关系

| 输入类型    | 中间过程                 | 最终输出       |
|---------|----------------------|------------|
| 用户自然语言  | 由 skill 在内部归一到受控请求形态 | 可直接执行的 HQL |
| 字段/语义约束 | 字段绑定、操作符决策、结果骨架约束    | 结构稳定的 HQL  |
| 已有 HQL  | 复审与一致性检查             | 审查结果       |

### 示例

```bash
cd hql-creator
python3 scripts/main.py --request '{
  "request_version": 1,
  "source": "告警",
  "time": {
    "relative": {
      "unit": "天",
      "value": 7
    }
  },
  "semantic_filters": [
    {
      "target": "模型研判结果",
      "operator": "==",
      "value": "攻击成功"
    }
  ],
  "field_filters": [
    {
      "field": "源地址",
      "operator": "belong",
      "value": "内网IP"
    }
  ],
  "result": {
    "type": "aggregate"
  }
}'
```

## 🧠 hql-creator

### 核心定位

`hql-creator` 的核心作用是：

| 项目     | 说明                        |
|--------|---------------------------|
| 面向对象   | 用户自然语言查询需求                |
| 核心目标   | 转换成可直接用于查询的 HQL 语句        |
| 核心价值   | 降低 Agent 直接手写 HQL 时的误判和漂移 |
| 当前实现特点 | 通过受控的请求形态、字段绑定和结果骨架来稳定输出  |

### 当前能力

| 能力        | 当前情况          |
|-----------|---------------|
| 自然语言到 HQL | ✅ 核心定位        |
| 字段检索      | ✅ 支持          |
| 结果骨架约束    | ✅ 支持          |
| HQL 复审    | ✅ 支持          |
| 调试输出      | ✅ 支持 `--json` |

### 文档入口

| 文档       | 链接                                                                                               |
|----------|--------------------------------------------------------------------------------------------------|
| Skill 文档 | [`hql-creator/SKILL.md`](./hql-creator/SKILL.md)                                                 |
| 请求契约     | [`hql-creator/references/request_contract.md`](./hql-creator/references/request_contract.md)     |
| 字段绑定说明   | [`hql-creator/references/field_overview.md`](./hql-creator/references/field_overview.md)         |
| 操作符说明    | [`hql-creator/references/hql_operator_guide.md`](./hql-creator/references/hql_operator_guide.md) |
| HQL 语法参考 | [`hql-creator/assets/hql-grammar-desc.htm`](./hql-creator/assets/hql-grammar-desc.htm)           |

---

## 🔎 hql-query

### 核心定位

`hql-query` 的核心作用是：

| 项目   | 说明                                   |
|------|--------------------------------------|
| 输入   | 已生成的 HQL 语句                          |
| 核心目标 | 把 HQL 发送给实际服务执行查询                    |
| 输出   | 查询结果与格式化后的结果展示                       |
| 主要价值 | 把“生成 HQL”和“执行查询/整理结果”拆成两个职责清晰的 skill |

---

## 📚 语法支持矩阵（hql-creator）

下面这部分是基于 [`hql-creator/assets/hql-grammar-desc.htm`](./hql-creator/assets/hql-grammar-desc.htm) 与当前实现边界做的客观映射。

### 🟢 完美支持

| 能力              | 支持度 | 说明                                                         |
|-----------------|-----|------------------------------------------------------------|
| 三类查询源           | 🟢  | 已明确支持 `日志`、`告警`、`原始告警`                                     |
| 时间过滤            | 🟢  | 支持 `preset`、`relative`、`between` 等常见结构                     |
| `search` 阶段字段过滤 | 🟢  | 主路径就是把条件编译成搜索过滤表达式                                         |
| 普通比较            | 🟢  | 支持 `== != > >= < <=`                                       |
| `like`          | 🟢  | 模糊匹配主路径之一                                                  |
| `rlike`         | 🟢  | 支持正则匹配，统一渲染为 `/正则/`                                        |
| `any_match`     | 🟢  | 多候选值匹配稳定可用                                                 |
| `belong`        | 🟢  | 重点能力之一，并有 `belong_rhs` 约束                                  |
| 明细查询            | 🟢  | `detail` 骨架已支持                                             |
| 总数统计            | 🟢  | `aggregate-total` 骨架已支持                                    |
| 单字段分组统计         | 🟢  | `aggregate-grouped` 骨架已支持                                  |
| Top-K 分组排行      | 🟢  | `aggregate-topk` 骨架已支持                                     |
| Top-K 后回明细      | 🟢  | `detail-topk` 骨架已支持                                        |
| 聚合函数            | 🟢  | 当前已覆盖 `count`、`avg`、`dc`、`estdc`、`max`、`min`、`range`、`sum` |
| 字段检索与字段绑定       | 🟢  | 支持字段名、key、alias、稳定短语，歧义时显式失败                               |
| HQL 复审          | 🟢  | 已提供 `review` 子命令                                           |

### 🔴 暂不支持或不建议承诺

| 能力                   | 支持度 | 说明                 |
|----------------------|-----|--------------------|
| 通用 `eval` 命令         | 🔴  | 不是当前公共请求入口能力       |
| 通用 `append` / `join` | 🔴  | 官方语法可见，但当前未开放成稳定能力 |

---

## 🗂️ 仓库结构

```text
hql-skills/
├── README.md
├── HQL_CREATOR_INSTALL.md
├── assets/
│   └── hql-creator-icon.png
├── hql-creator/
│   ├── SKILL.md
│   ├── requirements.txt
│   ├── agents/
│   ├── assets/
│   ├── references/
│   └── scripts/
└── hql-query/
```

## 🔗 文档入口

| 文档                     | 链接                                                                                               |
|------------------------|--------------------------------------------------------------------------------------------------|
| 安装说明                   | [`HQL_CREATOR_INSTALL.md`](installation.md)                                             |
| `hql-creator` Skill 文档 | [`hql-creator/SKILL.md`](./hql-creator/SKILL.md)                                                 |
| 公共请求契约                 | [`hql-creator/references/request_contract.md`](./hql-creator/references/request_contract.md)     |
| Source 路由规则            | [`hql-creator/references/source_routing.md`](./hql-creator/references/source_routing.md)         |
| 字段概览                   | [`hql-creator/references/field_overview.md`](./hql-creator/references/field_overview.md)         |
| HQL 操作符说明              | [`hql-creator/references/hql_operator_guide.md`](./hql-creator/references/hql_operator_guide.md) |
| HQL 语法参考               | [`hql-creator/assets/hql-grammar-desc.htm`](./hql-creator/assets/hql-grammar-desc.htm)           |

## 🛠️ 设计原则

| 原则            | 说明                |
|---------------|-------------------|
| 先拆 Skill 再扩能力 | 把生成、查询、格式化等职责分开   |
| 先保稳定性再扩自由度    | 优先输出可靠结果，而不是开放式乱拼 |
| 先约束输入再生成输出    | 避免直接猜 HQL 带来的漂移   |
| 不夸大当前状态       | 已实现和规划中的内容分开写清楚   |
