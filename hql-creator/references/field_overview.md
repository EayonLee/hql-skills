# 字段绑定说明

这份文档只回答一个问题：**公共请求里的字段引用如何绑定到真实字段。**

在这些场景下读取它：

- 字段命中不稳定
- 需要运行 `lookup-fields`
- 不确定 `field_filters` 该用普通比较 / `any_match` / `belong`

当你在 `belong` 和其他操作符之间拿不准时，不要先猜正则。固定按下面顺序判断：

1. 先把“模型研判结果 / 人工研判结果 / 综合研判结果”这类业务语义条件移到 `semantic_filters`
2. 再看剩余条件是不是“某个实体字段属于某个业务范围”
3. 如果是，再看当前 source 中是否存在可以绑定到该短语的 `belong_rhs=true` 字段
4. 如果存在，优先用 `belong`
5. 只有前面几种都不适合时，才考虑 `rlike`

## 哪些位置会做字段绑定

同一套字段解析器会用于：

- `field_filters[].field`
- `result.projection[]`
- `result.group_by`
- `result.metric.field`
- `field_filters[].value`，仅当 `operator="belong"` 时

## 字段可以怎么写

字段引用可以写成：

- 字段名
- field key
- 稳定 alias
- 稳定短语

例如：

- `源地址`
- `src_ip`
- `HTTP Cookie`
- `攻击地址`

## 绑定顺序

解析顺序固定：

1. 精确字段名
2. 精确 field key
3. 精确 alias
4. RapidFuzz 兜底

只有候选足够清晰时才会接受模糊匹配。  
否则直接报歧义，不会静默选第一个候选。

## 会用到哪些字段元数据

当前公共主路径会用到这些字段元数据：

- `type`
- `array`
- `belong_rhs`

### `array`

它表示字段本身是不是数组字段。

- 如果字段是数组，`any_match` 仍然是自然写法
- 如果字段是标量，`any_match` 也可以用，语义是“字段值命中候选值列表中的任意一个”

### `belong_rhs`

它表示字段能不能作为 `belong` 的右侧业务归属字段。

- `belong` 左侧应是实体字段，例如 `源地址`、`域名`、`用户`
- `belong` 右侧必须绑定到 `belong_rhs=true` 的字段
- 这就是系统区分“实体字段”和“业务归属字段”的方式
- 当自然语言里出现“属于信息组、内网IP”等业务归属判断时，优先选择 `belong`
- 如果当前 source 里没有合适的 `belong_rhs` 字段，再考虑别的操作符，而不是先默认写 `rlike`

典型正确写法：

```json
{
  "field": "源地址",
  "operator": "belong",
  "value": "内网IP"
}
```

典型错误写法：

```json
{
  "field": "内网IP",
  "operator": "belong",
  "value": "内网IP"
}
```

上面这个错误的根因是把右侧业务归属字段错当成了左侧实体字段。

## 边界

- 原始记录使用 `projection=[]`
- “攻击地址”会有意绑定到 `源地址`
- `metric.field` 也走同一套字段绑定
- `被模型研判过`、`模型研判结果` 这类内容优先走 `semantic_macros` / `semantic_filters`
- 字段歧义必须失败，这是刻意保留的正确性边界

## 命令

```bash
python3 scripts/main.py lookup-fields 告警 攻击地址
python3 scripts/main.py lookup-fields 告警 威胁类型
python3 scripts/main.py lookup-fields 日志 HTTP Cookie
python3 scripts/main.py lookup-fields 日志 src_ip
python3 scripts/main.py lookup-fields --all 进程路径
```
