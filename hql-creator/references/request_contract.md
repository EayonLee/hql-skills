# DraftRequestV1 请求契约

这份文档只描述公共请求契约，也就是：

```bash
python3 scripts/main.py --request '<json>'
```

它不解释 planner 内部实现，也不解释 reviewer 内部实现。

## 先选骨架

| 骨架                  | 适用场景                          |
|---------------------|-------------------------------|
| `detail`            | 最终结果是原始记录或明细投影                |
| `aggregate-total`   | 用户问“有多少个 / 总数 / 总量”，且不要求按字段分组 |
| `aggregate-grouped` | 用户要求按一个字段分组统计                 |
| `aggregate-topk`    | 用户要求分组统计，但只保留前 N 个组           |
| `detail-topk`       | top-k 只是中间步骤，最终还要回明细          |

## 标准骨架

### `detail`

```json
{
  "request_version": 1,
  "source": "日志",
  "time": {
    "preset": "今天"
  },
  "semantic_macros": [],
  "semantic_filters": [],
  "field_filters": [],
  "result": {
    "type": "detail",
    "projection": []
  },
  "show_intermediate": false,
  "raw_query": "原始中文问题"
}
```

### `aggregate-total`

```json
{
  "request_version": 1,
  "source": "告警",
  "time": {
    "relative": {
      "unit": "月",
      "value": 2
    }
  },
  "semantic_macros": [
    "被模型研判过"
  ],
  "semantic_filters": [],
  "field_filters": [],
  "result": {
    "type": "aggregate"
  },
  "show_intermediate": false,
  "raw_query": "最近两个月被模型研判过的告警有多少个？"
}
```

带业务归属字段过滤的总数统计也走同一个骨架，例如：

```json
{
  "request_version": 1,
  "source": "告警",
  "time": {
    "relative": {
      "unit": "天",
      "value": 7
    }
  },
  "semantic_macros": [],
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
  },
  "show_intermediate": false,
  "raw_query": "最近七天有多少内网IP被模型研判为攻击成功？"
}
```

### `aggregate-grouped`

```json
{
  "request_version": 1,
  "source": "告警",
  "time": {
    "relative": {
      "unit": "天",
      "value": 30
    }
  },
  "semantic_macros": [],
  "semantic_filters": [],
  "field_filters": [],
  "result": {
    "type": "aggregate",
    "group_by": "攻击地址"
  },
  "show_intermediate": false,
  "raw_query": "原始中文问题"
}
```

### `aggregate-topk`

```json
{
  "request_version": 1,
  "source": "告警",
  "time": {
    "relative": {
      "unit": "天",
      "value": 30
    }
  },
  "semantic_macros": [],
  "semantic_filters": [],
  "field_filters": [],
  "result": {
    "type": "aggregate",
    "group_by": "威胁类型",
    "top_k": {
      "limit": 5,
      "direction": "desc"
    }
  },
  "show_intermediate": false,
  "raw_query": "原始中文问题"
}
```

### `detail-topk`

```json
{
  "request_version": 1,
  "source": "告警",
  "time": {
    "relative": {
      "unit": "天",
      "value": 30
    }
  },
  "semantic_macros": [
    "被处置过"
  ],
  "semantic_filters": [],
  "field_filters": [],
  "result": {
    "type": "detail",
    "projection": [
      "攻击地址"
    ],
    "group_by": "威胁类型",
    "top_k": {
      "limit": 3,
      "direction": "desc"
    }
  },
  "show_intermediate": false,
  "raw_query": "原始中文问题"
}
```

## 顶层字段

### `request_version`

- 固定为 `1`

### `source`

- 只推荐这三个中文值：
    - `日志`
    - `告警`
    - `原始告警`
- 如果不确定 `source`，再读 [source_routing.md](source_routing.md)

### `time`

- 可省略；省略表示无时间过滤
- 标准时间只有三类：

```json
{
  "preset": "今天"
}
{
  "relative": {
    "unit": "天",
    "value": 30
  }
}
{
  "between": {
    "from": "2026-04-18 00:00:00",
    "to": "2026-04-18 23:59:59"
  }
}
```

#### `preset`

允许值：

- `今天`
- `昨天`
- `本周`
- `本月`
- `今年`

#### `relative.unit`

允许值：

- `分钟`
- `小时`
- `天`
- `周`
- `月`
- `年`

### `semantic_macros`

用于快捷业务语义，例如：

- `被模型研判过`
- `被处置过`
- `进程类告警`

### `semantic_filters`

用于带明确取值的业务语义条件：

```json
{
  "target": "模型研判结果",
  "operator": "==",
  "value": "攻击成功"
}
```

公开业务目标名：

- `模型研判结果`
- `人工研判结果`
- `综合研判结果`

### `field_filters`

用于显式字段条件。

```json
{
  "field": "源地址",
  "operator": "==",
  "value": "1.1.1.1"
}
```

字段过滤对象固定包含这三个字段：

- `field`
- `operator`
- `value`

`field` 可以写：

- 字段名
- field key
- 稳定 alias
- 稳定短语

操作符分三类：

- 普通比较：`== != > >= < <= like rlike`
- 多候选值匹配：`any_match`
- 业务归属判断：`belong`

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

形状规则：

- `any_match.value` 的标准写法是数组
- `any_match` 表示“左侧字段值命中右侧候选列表中的任意一个”
- 左侧如果恰好是数组字段，也同样可以用 `any_match`
- `rlike.value` 表示正则模式；可以写 `/正则/`，也可以写纯正则文本；系统会统一渲染成 `/正则/`
- `belong.value` 必须是单个字段短语字符串
- `belong.value` 不是字面量
- `belong.value` 不是数组
- `belong.value` 必须绑定到当前 source 中的业务归属字段
- 业务归属词例如“内网IP / 信息组”等等业务范围归属时优先使用 `belong`

字段绑定细节见 [field_overview.md](field_overview.md)。  
操作符语义见 [hql_operator_guide.md](hql_operator_guide.md)。

### `result`

标准结果字段：

- `type`
- `projection`
- `group_by`
- `metric`
- `top_k`
- `sort`

组合规则：

- `type="detail"` + 无 `group_by` + 无 `top_k` => 明细
- `type="aggregate"` + 无 `group_by` => 总数统计
- `type="aggregate"` + `group_by` => 分组统计
- `type="aggregate"` + `group_by` + `top_k` => 分组排行
- `type="detail"` + `group_by` + `top_k` => 先 top-k，再回明细

形状规则：

- `projection` 只用于 `detail`
- 省略 `projection` 或使用 `[]` 表示原始记录
- 总数统计时省略 `group_by`
- `group_by` 必须是单个字符串，不是数组
- `top_k` 只有在存在 `group_by` 时才合法
- 默认指标是 `count(ID) AS 数量`
- metric 使用结构化对象；默认指标是 `count(ID) AS 数量`
