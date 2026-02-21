# HAMi Scheduler 快速参考

## 调度流程速查

```
用户提交 Pod
    ↓
[1] Webhook 准入控制 (webhook.go)
    - 检查资源请求
    - 设置 schedulerName
    - 验证配额
    ↓
Kubernetes 调度器触发
    ↓
[2] Filter 过滤 (scheduler.go)
    - 获取节点使用情况 (getNodesUsage)
    - 计算节点分数 (calcScore)
    - 设备匹配 (fitInDevices)
    - 选择最优节点
    - 写入 Pod 注解
    ↓
[3] Bind 绑定 (scheduler.go)
    - 锁定节点
    - 执行绑定
    - 释放锁
    ↓
Pod 调度到节点
```

## 流程标记速查表

| 标记 | 函数 | 文件 | 说明 |
|------|------|------|------|
| `entry` | `start()` | `cmd/scheduler/main.go` | 程序入口 |
| `0` | `NewScheduler()` | `pkg/scheduler/scheduler.go` | 初始化调度器 |
| `0.1` | `RegisterFromNodeAnnotations()` | `pkg/scheduler/scheduler.go` | 节点注册循环 |
| `1` | `NewWebHook()` | `pkg/scheduler/webhook.go` | 创建 Webhook |
| `1.1` | `Handle()` | `pkg/scheduler/webhook.go` | Webhook 处理 |
| `1.2` | `fitResourceQuota()` | `pkg/scheduler/webhook.go` | 配额检查 |
| `2` | `Filter()` | `pkg/scheduler/scheduler.go` | 过滤接口 |
| `2.0` | `WaitForCacheSync()` | `pkg/scheduler/scheduler.go` | 等待同步 |
| `2.1` | `getNodesUsage()` | `pkg/scheduler/scheduler.go` | 获取使用情况 |
| `2.2` | `calcScore()` | `pkg/scheduler/score.go` | 计算分数 |
| `2.2.1` | `fitInDevices()` | `pkg/scheduler/score.go` | 设备匹配 |
| `2.2.2` | `ComputeScore()` | `pkg/scheduler/policy/gpu_policy.go` | 设备打分 |
| `2.2.3` | `OverrideScore()` | `pkg/scheduler/policy/node_policy.go` | 最终打分 |
| `2.2.4` | `ComputeDefaultScore()` | `pkg/scheduler/policy/node_policy.go` | 基础打分 |
| `3` | `Bind()` | `pkg/scheduler/scheduler.go` | 绑定接口 |

## 关键命令速查

### 查找流程标记
```bash
# 查找所有流程标记
grep -rn "//+scheduler:" pkg/scheduler/ cmd/scheduler/

# 查找特定步骤
grep -rn "//+scheduler:2" pkg/scheduler/

# 按顺序列出所有标记
grep -rh "//+scheduler:" pkg/scheduler/ cmd/scheduler/ | sort -u
```

### 查看调度器日志
```bash
# 查看所有日志
kubectl logs -n kube-system -l app=hami-scheduler

# 查看 Filter 相关日志
kubectl logs -n kube-system -l app=hami-scheduler | grep "Filter"

# 查看失败原因
kubectl logs -n kube-system -l app=hami-scheduler | grep "Failed\|Error"

# 实时查看日志
kubectl logs -n kube-system -l app=hami-scheduler -f
```

### 查看 Pod 调度状态
```bash
# 查看 Pod 事件
kubectl describe pod <pod-name>

# 查看 Pod 注解（设备分配信息）
kubectl get pod <pod-name> -o yaml | grep -A 10 "annotations:"

# 查看调度器名称
kubectl get pod <pod-name> -o jsonpath='{.spec.schedulerName}'
```

### 查看节点设备信息
```bash
# 查看节点注解（设备信息）
kubectl get node <node-name> -o yaml | grep -A 50 "annotations:"

# 查看节点资源
kubectl describe node <node-name> | grep -A 10 "Allocatable:"

# 查看所有节点的设备
kubectl get nodes -o custom-columns=NAME:.metadata.name,GPU:.status.allocatable."nvidia\.com/gpu"
```

## 核心数据结构速查

### Scheduler
```go
type Scheduler struct {
    nodeManager   *nodeManager      // 节点设备管理
    podManager    *PodManager       // Pod 分配记录
    quotaManager  *QuotaManager     // 配额管理
    cachedstatus  map[string]*NodeUsage   // 候选节点状态
    overviewstatus map[string]*NodeUsage  // 所有节点状态
}
```

### NodeUsage
```go
type NodeUsage struct {
    Node    *corev1.Node           // K8s 节点对象
    Devices policy.DeviceUsageList // 设备使用列表
}
```

### DeviceUsage
```go
type DeviceUsage struct {
    ID        string  // 设备 UUID
    Used      int32   // 已分配数量
    Usedmem   int32   // 已用显存 (MB)
    Usedcores int32   // 已用算力 (%)
    Count     int32   // 设备总数
    Totalmem  int32   // 总显存 (MB)
    Totalcore int32   // 总算力 (%)
}
```

### NodeScore
```go
type NodeScore struct {
    NodeID  string              // 节点名称
    Node    *corev1.Node        // 节点对象
    Devices device.PodDevices   // 分配的设备
    Score   float32             // 节点分数
}
```

## 调度策略速查

### 节点级策略

| 策略 | 行为 | 适用场景 |
|------|------|---------|
| **Binpack** (默认) | 优先使用已分配的节点 | 节省成本、提高利用率 |
| **Spread** | 优先使用空闲节点 | 高可用、分散风险 |

**配置方式**：
```yaml
# 全局配置
--node-scheduler-policy=binpack

# Pod 注解
annotations:
  hami.io/node-scheduler-policy: "spread"
```

### 设备级策略

| 策略 | 行为 | 适用场景 |
|------|------|---------|
| **Binpack** | 优先使用已分配的设备 | 减少碎片、同 NUMA |
| **Spread** (默认) | 优先使用空闲设备 | 均衡负载、跨 NUMA |

**配置方式**：
```yaml
# 全局配置
--gpu-scheduler-policy=spread

# Pod 注解
annotations:
  hami.io/gpu-scheduler-policy: "binpack"
```

## 打分公式速查

### 设备分数
```
score = weight × (usedScore + coreScore + memScore)

其中：
- usedScore = (request + used) / count
- coreScore = (requestCore + usedCore) / totalCore
- memScore = (requestMem + usedMem) / totalMem
```

### 节点分数
```
score = weight × (useScore + coreScore + memScore)

其中：
- useScore = used / total
- coreScore = usedCore / totalCore
- memScore = usedMem / totalMem
```

### 排序规则

**Binpack 策略**：
```go
// 分数低的排前面，实际选择分数高的（最后一个）
func Less(i, j int) bool {
    return score[i] < score[j]
}
```

**Spread 策略**：
```go
// 分数高的排前面，实际选择分数高的（最后一个）
func Less(i, j int) bool {
    return score[i] > score[j]
}
```

## 常见问题速查

### Pod Pending

**检查步骤**：
1. 查看 Pod 事件：`kubectl describe pod <pod-name>`
2. 查看调度器日志：`kubectl logs -n kube-system -l app=hami-scheduler | grep <pod-name>`
3. 检查节点设备：`kubectl get nodes -o yaml | grep -A 20 "hami.io"`

**常见原因**：
- ❌ 资源不足：`NodeInsufficientDevice`
- ❌ 配额超限：`exceeding resource quota`
- ❌ 节点不健康：`node unregistered`
- ❌ 设备不匹配：`Device type not found`

### 设备分配不均

**检查步骤**：
1. 查看调度策略：`kubectl get deployment -n kube-system hami-scheduler -o yaml | grep policy`
2. 查看节点使用情况：访问 Prometheus 指标

**解决方案**：
- 使用 Spread 策略：`--node-scheduler-policy=spread`
- 使用 Pod 注解覆盖：`hami.io/node-scheduler-policy: "spread"`

### 调度器不响应

**检查步骤**：
1. 查看 Pod 状态：`kubectl get pod -n kube-system -l app=hami-scheduler`
2. 查看 Readiness：`kubectl get pod -n kube-system <scheduler-pod> -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'`
3. 查看 Leader 状态：`kubectl logs -n kube-system <scheduler-pod> | grep leader`

**常见原因**：
- ❌ 缓存未同步：等待 Informer 同步完成
- ❌ 不是 Leader：检查 Leader 选举状态
- ❌ 证书过期：检查 TLS 证书

## HTTP 接口速查

### Webhook 接口
```
POST /webhook
Content-Type: application/json

请求：AdmissionReview
响应：AdmissionReview (with patches)
```

### Filter 接口
```
POST /filter
Content-Type: application/json

请求：ExtenderArgs
响应：ExtenderFilterResult
```

### Bind 接口
```
POST /bind
Content-Type: application/json

请求：ExtenderBindingArgs
响应：ExtenderBindingResult
```

### 健康检查
```
GET /healthz    # Liveness 探针
GET /readyz     # Readiness 探针
```

### 监控指标
```
GET /metrics    # Prometheus 指标
```

### 性能分析（需启用 --profiling）
```
GET /debug/pprof/           # 概览
GET /debug/pprof/heap       # 堆内存
GET /debug/pprof/goroutine  # Goroutine
GET /debug/pprof/profile    # CPU (30秒)
```

## 配置参数速查

### 调度器配置
```bash
--scheduler-name=hami-scheduler          # 调度器名称
--node-scheduler-policy=binpack          # 节点策略
--gpu-scheduler-policy=spread            # GPU 策略
--default-mem=0                          # 默认显存 (MB)
--default-cores=0                        # 默认算力 (%)
--default-gpu=1                          # 默认 GPU 数量
```

### 网络配置
```bash
--http_bind=127.0.0.1:8080              # HTTP 监听地址
--cert_file=/path/to/cert.pem           # TLS 证书
--key_file=/path/to/key.pem             # TLS 密钥
--metrics-bind-address=:9395            # 指标监听地址
```

### Kubernetes 客户端配置
```bash
--kube-qps=50.0                         # API 请求 QPS
--kube-burst=100                        # API 请求突发
--kube-timeout=30                       # API 请求超时 (秒)
```

### Leader 选举配置
```bash
--leader-elect=true                     # 启用 Leader 选举
--leader-elect-resource-name=hami-scheduler-leader
--leader-elect-resource-namespace=kube-system
```

### 其他配置
```bash
--node-lock-timeout=5m                  # 节点锁超时
--profiling=false                       # 启用性能分析
--node-label-selector=key=value         # 节点标签过滤
```

## 注解速查

### Pod 注解

**调度策略**：
```yaml
hami.io/node-scheduler-policy: "binpack"  # 节点策略
hami.io/gpu-scheduler-policy: "spread"    # GPU 策略
```

**设备分配结果**（由调度器写入）：
```yaml
hami.io/assigned-node: "node-1"           # 分配的节点
hami.io/assigned-time: "1234567890"       # 分配时间戳
hami.io/vgpu-devices-allocated: "..."     # 分配的设备详情
hami.io/device-bind-phase: "allocating"   # 绑定阶段
hami.io/bind-time: "1234567890"           # 绑定时间戳
```

### 节点注解

**设备信息**（由 Device Plugin 写入）：
```yaml
hami.io/vgpu-devices-to-allocate: "..."   # 可分配的设备列表
hami.io/node-handshake: "..."             # 握手信息
```

## 监控指标速查

### 设备指标
```
hami_device_total{node="node-1",device="GPU-xxx"}           # 设备总数
hami_device_used{node="node-1",device="GPU-xxx"}            # 已用设备数
hami_device_memory_total{node="node-1",device="GPU-xxx"}    # 总显存 (MB)
hami_device_memory_used{node="node-1",device="GPU-xxx"}     # 已用显存 (MB)
hami_device_core_total{node="node-1",device="GPU-xxx"}      # 总算力 (%)
hami_device_core_used{node="node-1",device="GPU-xxx"}       # 已用算力 (%)
```

### 调度指标
```
hami_schedule_filter_total{result="success"}                # Filter 总次数
hami_schedule_filter_duration_seconds{result="success"}     # Filter 耗时
hami_schedule_bind_total{result="success"}                  # Bind 总次数
hami_schedule_bind_duration_seconds{result="success"}       # Bind 耗时
```

## 日志级别速查

| 级别 | 用途 | 示例 |
|------|------|------|
| `V(1)` | 重要事件 | Pod 调度成功/失败 |
| `V(2)` | 详细信息 | 节点分数计算 |
| `V(4)` | 调试信息 | 节点过滤结果 |
| `V(5)` | 详细调试 | 设备状态详情 |

**设置日志级别**：
```bash
--v=4  # 显示 V(1) 到 V(4) 的日志
```

## 故障排查速查

### 调度器启动失败
```bash
# 查看 Pod 状态
kubectl get pod -n kube-system -l app=hami-scheduler

# 查看启动日志
kubectl logs -n kube-system <scheduler-pod>

# 常见原因
- 配置文件错误
- 证书文件不存在
- 端口被占用
- RBAC 权限不足
```

### 设备未注册
```bash
# 查看节点注解
kubectl get node <node-name> -o yaml | grep hami.io

# 查看 Device Plugin 日志
kubectl logs -n kube-system -l app=hami-device-plugin

# 常见原因
- Device Plugin 未运行
- 设备驱动未安装
- 节点标签不匹配
```

### 性能问题
```bash
# 启用 Profiling
--profiling=true

# 访问性能分析
curl http://localhost:8080/debug/pprof/

# 分析 CPU
go tool pprof http://localhost:8080/debug/pprof/profile

# 分析内存
go tool pprof http://localhost:8080/debug/pprof/heap
```

## 开发调试速查

### 本地运行
```bash
# 编译
make build

# 运行
./bin/scheduler \
  --scheduler-name=hami-scheduler \
  --http_bind=127.0.0.1:8080 \
  --v=4
```

### 单元测试
```bash
# 运行所有测试
make test

# 运行特定包的测试
go test -v ./pkg/scheduler/...

# 运行特定测试
go test -v -run TestFilter ./pkg/scheduler/
```

### 代码检查
```bash
# 格式化代码
gofmt -w .

# 静态检查
golangci-lint run

# 查看测试覆盖率
go test -cover ./pkg/scheduler/...
```

## 相关文档

- 📖 **架构文档**：`docs/scheduler-architecture-zh.md`
- 📚 **代码指南**：`docs/scheduler-code-guide-zh.md`
- 🔍 **快速参考**：`docs/scheduler-quick-reference-zh.md`（本文档）

---

**提示**：将本文档保存为书签，方便随时查阅！

