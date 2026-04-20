# 自然语言到 HQL 的模式

这份文档只给维护者看，用来维护 prompt 和 planner。  
它不是公共契约。

每个模式都描述“自然语言意图 -> DraftRequestV1 形态”。

## 1. 原始记录 / 明细

自然语言：

- “查今天这个源地址的日志”
- “给我这些告警明细”

请求形态：

- `result.type = "detail"`
- 无 `group_by`
- 无 `top_k`

## 2. 总数统计

自然语言：

- “有多少个”
- “总数是多少”
- “最近两个月被模型研判过的告警有多少个”

请求形态：

- `result.type = "aggregate"`
- 省略 `group_by`

## 3. 分组统计

自然语言：

- “按攻击地址统计数量”
- “统计每个威胁类型的数量”

请求形态：

- `result.type = "aggregate"`
- `result.group_by = "<字段>"`

## 4. 分组 top-k

自然语言：

- “数量最多的前 5 个威胁类型”
- “攻击地址排名前 10”

请求形态：

- `result.type = "aggregate"`
- `result.group_by = "<字段>"`
- `result.top_k.limit = N`

## 5. 先 top-k，再回明细

自然语言：

- “找出数量最多的威胁类型对应的所有攻击地址”
- “先找 top 1，再回所有明细”

请求形态：

- `result.type = "detail"`
- `result.group_by = "<字段>"`
- `result.top_k.limit = N`
- `result.projection` 填最终要返回的明细字段

## 6. 业务语义

自然语言：

- “被模型研判过”
- “攻击成功”
- “被处置过”

请求形态：

- 快捷业务语义 -> `semantic_macros`
- 带值业务语义 -> `semantic_filters`

## 7. 操作符选择

自然语言：

- 普通标量比较 -> 普通比较
- 通配模糊匹配 -> `like`
- 字段值需要命中多个候选值之一 -> `any_match`
- 字段需要判断是否属于业务归属字段 -> `belong`
- 只有其他操作符都不适合表达时，才用 `rlike`

判断 `belong` 的通用规则：

- 如果右侧是“信息组 / 内网IP 等等某业务范围”这类业务归属概念
- 并且当前 source 存在可绑定的 `belong_rhs` 字段
- 那就优先选择 `belong`

示例：

```json
{
  "field": "标签",
  "operator": "any_match",
  "value": [
    "高危",
    "外连"
  ]
}
{
  "field": "源地址",
  "operator": "belong",
  "value": "内网IP"
}
```
