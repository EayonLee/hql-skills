---
name: hql-creator
description: 用 `python3 scripts/main.py` 把日志、告警、原始告警查询需求转换成 DraftRequestV1 并生成 HQL，或复审现有 HQL 是否满足结构化请求。适用于：生成 HQL、把中文查询改写成结构化请求、或按结构化请求复审 HQL。
---

# HQL Creator

用这个 skill 做两类事：

- 把查询需求转换成 DraftRequestV1 并生成 HQL
- 已经有 HQL 时，按结构化请求复审它是否满足需求

## 主入口

本 skill 的根目录就是 **当前 `SKILL.md` 所在目录**。  
`scripts/`、`references/` 等相对路径都相对这个 skill 根目录解析，不相对用户项目目录或当前工作目录解析。

执行任何命令时，都默认在 skill 根目录运行。更稳的写法是：

```bash
cd <skill-root> && python3 scripts/main.py ...
```

公共生成入口只有一个：

```bash
cd <skill-root> && python3 scripts/main.py --request '<DraftRequestV1 JSON>'
```

结果形态拿不准时，先打印骨架：

```bash
cd <skill-root> && python3 scripts/main.py skeleton detail
cd <skill-root> && python3 scripts/main.py skeleton aggregate-total
cd <skill-root> && python3 scripts/main.py skeleton aggregate-grouped
cd <skill-root> && python3 scripts/main.py skeleton aggregate-topk
cd <skill-root> && python3 scripts/main.py skeleton detail-topk
```

## 先选结果骨架

- `detail`
  最终结果是原始记录或明细投影
- `aggregate-total`
  用户问“有多少个 / 总数 / 总量”，且不要求按字段分组
- `aggregate-grouped`
  用户要求按一个字段分组统计
- `aggregate-topk`
  用户要求分组统计，但只保留前 N 个组
- `detail-topk`
  top-k 只是中间步骤，最终还要回明细

## 工作流

1. 先选结果骨架
2. 再填写 `source`、可选 `time`、`semantic_macros` / `semantic_filters` / `field_filters`
3. 在 skill 根目录中运行 `main.py --request`
4. 成功即停

失败时最多只做一次额外动作：

- 改用 `--json` 重新看错误，或
- 重新打印对应骨架再构造请求

## 输出

`main.py` 输出的 HQL 单行就是最终产物。最终回复固定使用下面的形式：

````text
生成HQL如下：
```sql
<逐字复制 `main.py` 标准输出中的 HQL 单行，不要作任何更改。>
```

说明：
- 只在确有必要时补 1 到 3 句简短说明
- 不要在说明里重复 HQL
- 不要提供第二个版本、等价版本、格式化版本或“修正版” HQL
````

硬规则：

- 把 `main.py` 标准输出当成只读产物。复制到 `sql` 代码块后，不要再编辑。
- `sql` 代码块中的每一个字符都必须与 `main.py` 标准输出完全一致。
- 禁止做任何格式化动作，包括：
  - 增加空格
  - 删除空格
  - 替换字段名
  - 替换标点
  - 替换括号
  - 替换大小写
  - 替换正则写法
- 中文、英文、数字混在一起的字段名也必须原样复制，不要按语言习惯重新分词。
- 正确：`内网IP`
- 错误：`内网 IP`
- 正确：`Windows系统进程`
- 错误：`Windows 系统进程`
- 不要在代码块前后再补第二条 HQL。

## 请求形状

固定写法：

- `request_version=1`
- `source` 只用：`日志` / `告警` / `原始告警`
- 没有时间范围时，省略 `time`
- 总数统计时，省略 `group_by`
- `group_by` 只能是单个字段字符串
- `field_filters` 固定写成 `{field, operator, value}`
- HQL 结构符号统一使用英文半角，例如 `,`、`()`、`[]`、`"`、`'` 等等

## 操作符决策

先判断条件属于哪一层，再选操作符。

### 第一步：先分层

1. 如果条件是业务语义，例如“被模型研判过 / 模型研判结果为攻击成功 / 人工研判结果为误报”，优先写进 `semantic_macros` 或 `semantic_filters`
2. 只有剩下的显式字段条件，才写进 `field_filters`

不要把“模型研判结果”“人工研判结果”写成普通字段比较。

### 第二步：字段条件再选操作符

按下面顺序判断，只选一个：

1. 如果是单值相等、不等或大小比较，用普通比较
2. 如果是通用模糊匹配，用 `like`
3. 如果字段值需要命中多个候选值之一，用 `any_match`
3. `any_match.value` 的标准写法是数组
4. 如果右侧表达的是业务归属范围，并且当前 source 存在可绑定的 `belong_rhs` 字段，用 `belong`
5. 只有前面四种都不适合时，才用 `rlike`

### 第三步：`belong` 固定检查左右角色

使用 `belong` 前，固定检查这三件事：

1. 左侧必须是实体字段，例如 `源地址`、`域名`、`用户`、`进程路径`
2. 右侧必须是业务归属字段，例如信息组、内网IP 等对应字段
3. 如果当前 source 里找不到可绑定的 `belong_rhs` 字段，就不要硬写 `belong`

标准形态：

```hql
源地址 belong 内网IP
```

不要写成：

```hql
内网IP belong 内网IP
内网IP belong 源地址
```

`rlike` 规则：

- `rlike.value` 写正则模式
- 系统会统一渲染成 `/正则/`
- 不要把“业务归属判断”退化成 `rlike`

## 必读参考资料

- [references/request_contract.md](references/request_contract.md)
    - 公共契约、字段形状、标准骨架
- [references/source_routing.md](references/source_routing.md)
    - `source` 不确定时再读
- [references/field_overview.md](references/field_overview.md)
    - 字段绑定或字段选择不确定时再读
- [references/hql_operator_guide.md](references/hql_operator_guide.md)
    - 操作符语义或渲染规则不确定时再读
- [references/nl2hql_patterns.md](references/nl2hql_patterns.md)
    - 维护 prompt 或 planner 时再读
