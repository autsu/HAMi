# HAMi Scheduler 源码学习指南

## 概述

本指南帮助你通过源码注释快速理解 HAMi Scheduler 的工作原理。所有关键函数都添加了详细的注释，并使用 `//+scheduler:X` 标记来标识调度流程中的步骤。

## 如何使用流程标记

### 流程标记格式

```go
//+scheduler:步骤编号
// 函数说明
func FunctionName() {
    // 实现
}
```

### 流程编号说明

| 标记 | 含义 | 文件位置 |
|------|------|---------|
| `//+scheduler:entry` | 程序入口 | `cmd/scheduler/main.go` |
| `//+scheduler:0` | 调度器初始化 | `pkg/scheduler/scheduler.go` |
| `//+scheduler:0.1` | 节点注册后台任务 | `pkg/scheduler/scheduler.go` |
| `//+scheduler:1` | Webhook 准入控制 | `pkg/scheduler/webhook.go` |
| `//+scheduler:1.1` | Webhook 处理逻辑 | `pkg/scheduler/webhook.go` |
| `//+scheduler:1.2` | 配额检查 | `pkg/scheduler/webhook.go` |
| `//+scheduler:1.http` | Webhook HTTP 路由 | `pkg/scheduler/routes/route.go` |
| `//+scheduler:2` | Filter 过滤接口 | `pkg/scheduler/scheduler.go` |
| `//+scheduler:2.0` | 缓存同步等待 | `pkg/scheduler/scheduler.go` |
| `//+scheduler:2.1` | 获取节点使用情况 | `pkg/scheduler/scheduler.go` |
| `//+scheduler:2.2` | 计算节点分数 | `pkg/scheduler/score.go` |
| `//+scheduler:2.2.1` | 设备匹配 | `pkg/scheduler/score.go` |
| `//+scheduler:2.2.2` | 设备打分 | `pkg/scheduler/policy/gpu_policy.go` |
| `//+scheduler:2.2.3` | 节点最终打分 | `pkg/scheduler/policy/node_policy.go` |
| `//+scheduler:2.2.4` | 节点基础打分 | `pkg/scheduler/policy/node_policy.go` |
| `//+scheduler:2.http` | Filter HTTP 路由 | `pkg/scheduler/routes/route.go` |
| `//+scheduler:3` | Bind 绑定接口 | `pkg/scheduler/scheduler.go` |
| `//+scheduler:3.http` | Bind HTTP 路由 | `pkg/scheduler/routes/route.go` |

## 学习路径

### 路径 1：完整调度流程（推荐新手）

按照 Pod 从创建到绑定的完整生命周期学习：

1. **程序启动** (`//+scheduler:entry`)
   ```bash
   # 查找入口函数
   grep -r "//+scheduler:entry" cmd/scheduler/
   ```
   - 文件：`cmd/scheduler/main.go`
   - 理解：如何初始化调度器、启动 HTTP 服务

2. **调度器初始化** (`//+scheduler:0`)
   ```bash
   grep -r "//+scheduler:0" pkg/scheduler/
   ```
   - 文件：`pkg/scheduler/scheduler.go`
   - 理解：三大管理器的作用、Leader 选举机制

3. **Webhook 准入控制** (`//+scheduler:1` 系列)
   ```bash
   grep -r "//+scheduler:1" pkg/scheduler/
   ```
   - 文件：`pkg/scheduler/webhook.go`
   - 理解：如何验证 Pod、设置调度器名称、检查配额

4. **Filter 过滤** (`//+scheduler:2` 系列)
   ```bash
   grep -r "//+scheduler:2" pkg/scheduler/
   ```
   - 文件：`pkg/scheduler/scheduler.go`, `pkg/scheduler/score.go`
   - 理解：如何选择最优节点、打分机制

5. **Bind 绑定** (`//+scheduler:3` 系列)
   ```bash
   grep -r "//+scheduler:3" pkg/scheduler/
   ```
   - 文件：`pkg/scheduler/scheduler.go`
   - 理解：如何锁定节点、执行绑定

### 路径 2：核心机制深入（推荐有经验者）

#### 2.1 调度策略

**节点级策略**：
- 文件：`pkg/scheduler/policy/node_policy.go`
- 关键函数：
  - `ComputeDefaultScore`: 基础打分
  - `OverrideScore`: 最终打分
  - `Less`: 排序规则

**设备级策略**：
- 文件：`pkg/scheduler/policy/gpu_policy.go`
- 关键函数：
  - `ComputeScore`: 设备打分
  - `Less`: 设备排序规则

**学习要点**：
- Binpack vs Spread 的区别
- 为什么需要两层策略
- NUMA 亲和性的影响

#### 2.2 节点管理

- 文件：`pkg/scheduler/nodes.go`
- 关键函数：
  - `addNode`: 注册节点设备
  - `GetNode`: 查询节点信息
  - `ListNodes`: 列出所有节点

**学习要点**：
- 如何从节点注解读取设备信息
- 为什么需要深拷贝
- 读写锁的使用

#### 2.3 设备使用情况计算

- 文件：`pkg/scheduler/scheduler.go`
- 关键函数：`getNodesUsage` (`//+scheduler:2.1`)

**学习要点**：
- 如何累加 Pod 的设备使用量
- MIG 设备的特殊处理
- overviewstatus vs cachedstatus

#### 2.4 节点锁机制

- 文件：`pkg/scheduler/scheduler.go`
- 关键函数：`Bind` (`//+scheduler:3`)

**学习要点**：
- 为什么需要节点锁
- 如何使用 Kubernetes Lease 实现分布式锁
- 锁超时的处理

### 路径 3：问题排查（推荐运维人员）

#### 3.1 Pod 一直 Pending

**排查步骤**：

1. 查看 Webhook 是否通过：
   ```bash
   kubectl describe pod <pod-name>
   # 查看 Events，是否有 "exceeding resource quota" 等错误
   ```

2. 查看 Filter 失败原因：
   ```bash
   # 查看调度器日志
   kubectl logs -n kube-system <scheduler-pod> | grep "FilteringFailed"
   ```
   - 关键代码：`pkg/scheduler/scheduler.go` 的 `Filter` 函数
   - 查找 `EventReasonFilteringFailed` 事件

3. 检查节点设备状态：
   ```bash
   # 查看节点注解
   kubectl get node <node-name> -o yaml | grep -A 20 "annotations:"
   ```
   - 关键代码：`pkg/scheduler/scheduler.go` 的 `getNodesUsage` 函数

#### 3.2 设备分配不均

**排查步骤**：

1. 检查调度策略：
   ```bash
   # 查看全局配置
   kubectl get deployment -n kube-system hami-scheduler -o yaml | grep "node-scheduler-policy"
   
   # 查看 Pod 注解
   kubectl get pod <pod-name> -o yaml | grep "hami.io/node-scheduler-policy"
   ```

2. 理解策略影响：
   - 关键代码：`pkg/scheduler/policy/node_policy.go` 的 `Less` 函数
   - Binpack：优先使用已分配的节点
   - Spread：优先使用空闲节点

#### 3.3 调度器不响应

**排查步骤**：

1. 检查 Readiness 状态：
   ```bash
   kubectl get pod -n kube-system <scheduler-pod>
   # 查看 READY 列
   ```

2. 检查 Leader 状态：
   ```bash
   kubectl logs -n kube-system <scheduler-pod> | grep "leader"
   ```
   - 关键代码：`pkg/scheduler/scheduler.go` 的 `WaitForCacheSync` 函数

3. 检查缓存同步：
   ```bash
   kubectl logs -n kube-system <scheduler-pod> | grep "synced"
   ```

## 代码阅读技巧

### 1. 使用 grep 快速定位

```bash
# 查找所有流程标记
grep -r "//+scheduler:" pkg/scheduler/

# 查找特定步骤
grep -r "//+scheduler:2" pkg/scheduler/

# 查找关键函数
grep -rn "func Filter" pkg/scheduler/
```

### 2. 理解数据流

**设备信息流**：
```
Device Plugin (节点)
  ↓ 写入 Node Annotation
NodeManager
  ↓ 解析并存储
getNodesUsage
  ↓ 计算使用情况
calcScore
  ↓ 打分排序
Filter
  ↓ 返回最优节点
```

**Pod 分配信息流**：
```
Filter
  ↓ 选择设备
PatchAnnotations
  ↓ 写入 Pod Annotation
Bind
  ↓ 绑定到节点
Device Plugin
  ↓ 读取注解
配置容器环境
```

### 3. 关注关键数据结构

**NodeUsage**：
```go
type NodeUsage struct {
    Node    *corev1.Node           // Kubernetes 节点对象
    Devices policy.DeviceUsageList // 设备使用列表
}
```

**DeviceUsage**：
```go
type DeviceUsage struct {
    ID        string  // 设备 UUID
    Used      int32   // 已分配数量
    Usedmem   int32   // 已用显存
    Usedcores int32   // 已用算力
    // ... 更多字段
}
```

**NodeScore**：
```go
type NodeScore struct {
    NodeID  string              // 节点名称
    Devices device.PodDevices   // 分配的设备
    Score   float32             // 节点分数
}
```

### 4. 理解并发控制

**读写锁**（nodeManager）：
```go
// 读操作（可并发）
func (m *nodeManager) GetNode(nodeID string) {
    m.mutex.RLock()
    defer m.mutex.RUnlock()
    // ...
}

// 写操作（独占）
func (m *nodeManager) addNode(nodeID string, nodeInfo *NodeInfo) {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    // ...
}
```

**并发打分**（calcScore）：
```go
for nodeID, node := range *nodes {
    wg.Add(1)
    go func(nodeID string, node *NodeUsage) {
        defer wg.Done()
        // 并发计算每个节点的分数
    }(nodeID, node)
}
wg.Wait()
```

## 常见问题解答

### Q1: 为什么需要 Webhook 和 Filter 两个阶段？

**A**: 分工不同：
- **Webhook**：准入控制，在 Pod 创建时验证和修改
  - 检查资源请求是否合法
  - 设置正确的 schedulerName
  - 验证配额
- **Filter**：调度决策，选择最优节点
  - 计算节点分数
  - 分配具体设备
  - 更新 Pod 注解

### Q2: 为什么 Filter 要删除 Pod 再添加？

**A**: 避免重复计算：
```go
// 同一个 Pod 可能多次调用 Filter（调度失败重试）
s.podManager.DelPod(args.Pod)  // 删除旧记录
// ... 执行调度 ...
s.podManager.AddPod(args.Pod, nodeID, devices)  // 添加新记录
```

### Q3: 为什么需要两个 status（overviewstatus 和 cachedstatus）？

**A**: 用途不同：
- **overviewstatus**：所有节点的完整状态
  - 用于监控指标
  - 用于全局视图
- **cachedstatus**：候选节点的状态
  - 只包含本次调度的候选节点
  - 减少不必要的计算

### Q4: 分数是越高越好还是越低越好？

**A**: 取决于策略：
- **Binpack**：分数低的排前面（Less 返回 score < score）
  - 排序后从后往前选（最后一个元素）
  - 实际选择的是分数高的（已使用多的）
- **Spread**：分数高的排前面（Less 返回 score > score）
  - 排序后从后往前选（最后一个元素）
  - 实际选择的是分数高的（空闲多的）

### Q5: 如何添加自定义调度策略？

**A**: 修改打分逻辑：

1. **设备级策略**：修改 `pkg/scheduler/policy/gpu_policy.go`
   ```go
   func (ds *DeviceListsScore) ComputeScore(requests device.ContainerDeviceRequests) {
       // 自定义打分逻辑
       ds.Score = customScore
   }
   ```

2. **节点级策略**：修改 `pkg/scheduler/policy/node_policy.go`
   ```go
   func (ns *NodeScore) OverrideScore(previous []*device.DeviceUsage, policy string) {
       // 自定义打分逻辑
       ns.Score += customScore
   }
   ```

3. **通过注解控制**：
   ```yaml
   annotations:
     hami.io/node-scheduler-policy: "custom"
     hami.io/gpu-scheduler-policy: "custom"
   ```

## 进阶学习

### 1. 扩展新设备类型

参考：`pkg/device/` 目录下的设备实现

关键接口：
```go
type Devices interface {
    MutateAdmission(ctr *corev1.Container, pod *corev1.Pod) (bool, error)
    CheckHealth(devType string, node *corev1.Node) (bool, bool)
    GetNodeDevices(node corev1.Node) ([]*DeviceInfo, error)
    Fit(devices []*DeviceUsage, request ContainerDeviceRequest, ...) (bool, ContainerDevices, string)
    // ... 更多方法
}
```

### 2. 自定义监控指标

参考：`cmd/scheduler/metrics.go`

添加新指标：
```go
prometheus.NewGaugeVec(
    prometheus.GaugeOpts{
        Name: "hami_custom_metric",
        Help: "Custom metric description",
    },
    []string{"label1", "label2"},
)
```

### 3. 性能优化

**并发优化**：
- Filter 阶段的节点打分已经并发
- 可以考虑并发处理容器的设备请求

**缓存优化**：
- 使用 cachedstatus 减少计算
- 考虑添加设备信息缓存

**算法优化**：
- 优化设备匹配算法
- 减少不必要的排序

## 总结

通过本指南，你应该能够：

1. ✅ 理解 HAMi Scheduler 的完整调度流程
2. ✅ 使用流程标记快速定位关键代码
3. ✅ 掌握核心数据结构和算法
4. ✅ 排查常见的调度问题
5. ✅ 扩展和定制调度策略

**下一步**：
- 阅读架构文档：`docs/scheduler-architecture-zh.md`
- 运行调度器并观察日志
- 尝试修改调度策略并测试效果
- 为新设备类型实现调度逻辑

**学习建议**：
- 从简单的场景开始（单容器、单设备）
- 逐步增加复杂度（多容器、多设备类型）
- 结合日志理解代码执行流程
- 动手修改代码并测试

祝你学习愉快！如有问题，欢迎查看源码注释或提交 Issue。

