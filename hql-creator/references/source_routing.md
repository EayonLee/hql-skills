/# 查询源选择

`source` 只负责选查询域。  
时间、字段条件和业务语义分别放到 `time`、`field_filters`、`semantic_macros` / `semantic_filters`。

## V1 标准值

公共请求只推荐三种中文值：

| 公共 source | lowering 后内部 source | 最终 index      | 默认时间字段 |
|-----------|---------------------|---------------|--------|
| `日志`      | `event`             | `event*`      | `发生时间` |
| `告警`      | `alarm_merge`       | `alarm_merge` | `开始时间` |
| `原始告警`    | `alarm`             | `alarm`       | `开始时间` |

## 选择规则

- 用户只说“告警”时，默认选 `告警`
- 只有明确强调“原始告警”时，才选 `原始告警`
- 用户说“日志”时，选 `日志`

## 边界

- `source` 只放查询域本身
- 业务语义放到 `semantic_macros` / `semantic_filters`
- 时间范围放到 `time`

## 统一约定

公共主路径和文档只使用上面的三种中文 source。
