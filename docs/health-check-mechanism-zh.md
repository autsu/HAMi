# HAMi 设备健康检查机制详解

## 概述

HAMi 支持多种异构设备（NVIDIA GPU、AMD GPU、Hygon DCU 等），不同设备类型有不同的健康检查实现方式。本文档详细说明各种健康检查机制的工作原理。

## 三种健康检查实现方式

### 1. NVIDIA GPU - 基于设备数量对比

**实现位置**: `pkg/device/nvidia/device.go:223`

**工作原理**:
1. 从节点的 `Status.Allocatable` 中读取当前设备数量（由 Device Plugin 上报）
2. 与上次记录的设备数量（`ReportedGPUNum`）进行对比
3. 根据对比结果判断设备健康状态

**判断逻辑**:

| 当前数量 | 上次记录 | 返回值 | 说明 | 后续动作 |
|---------|---------|--------|------|---------|
| 0 | 0 | `(true, false)` | 节点从未有过设备，正常状态 | 无 |
| 0 | > 0 | `(false, false)` | 设备消失！可能是 Device Plugin 崩溃 | 调用 `NodeCleanUp` + `rmNodeDevices` |
| > 0 | != current | `(true, true)` | 设备数量变化，需要更新 | 重新读取节点注解 |
| > 0 | == current | `(true, false)` | 设备数量未变化，一切正常 | 无 |

**优点**:
- 简单直接，不依赖额外的握手机制
- 实时性好，直接读取 Kubernetes 资源状态
- 适合设备数量稳定的场景

**代码示例**:
```go
func (dev *NvidiaGPUDevices) CheckHealth(devType string, n *corev1.Node) (bool, bool) {
    current := int64(0)
    quantity := n.Status.Allocatable.Name(corev1.ResourceName(dev.config.ResourceCountName), resource.DecimalSI)
    if quantity != nil {
        current = quantity.Value()
    }

    dev.mu.Lock()
    defer dev.mu.Unlock()

    reported := dev.ReportedGPUNum[n.Name]

    if current == 0 {
        if reported == 0 {
            return true, false  // 从未有过设备
        }
        dev.ReportedGPUNum[n.Name] = current
        return false, false  // 设备消失了！
    }

    if reported != current {
        dev.ReportedGPUNum[n.Name] = current
        return true, true  // 设备数量变化，需要更新
    }

    return true, false  // 一切正常
}
```

### 2. Hygon DCU - 基于 HandshakeAnnos 握手机制

**实现位置**: `pkg/device/hygon/device.go:183`（调用通用函数 `device.CheckHealth`）

**工作原理**:
通过节点注解 `HandshakeAnnos` 实现 Scheduler 和 Device Plugin 之间的握手协调。

**HandshakeAnnos 的三种状态**:

#### 状态 1: 空或其他值（首次检查或需要重新握手）

```
节点注解: hami.io/node-handshake-dcu = "" 或其他值

Scheduler 行为:
1. 打上时间戳: "Requesting_2026-02-22 10:30:00"
2. 返回: (true, true) - 健康且需要更新

Device Plugin 行为:
1. 监听到注解变化
2. 重新上报设备信息
3. 更新注解为新值（表示已响应）
```

#### 状态 2: "Requesting_时间戳"（等待 Device Plugin 响应）

```
节点注解: hami.io/node-handshake-dcu = "Requesting_2026-02-22 10:30:00"

Scheduler 行为:
1. 解析时间戳
2. 检查是否超时（60 秒）
   - 未超时: 返回 (true, false) - 健康但不更新
   - 超时: 返回 (false, false) - 不健康，触发清理

超时原因:
- Device Plugin 崩溃
- Device Plugin 响应慢
- 网络问题导致注解更新失败
```

#### 状态 3: "Deleted"（设备已下线）

```
节点注解: hami.io/node-handshake-dcu = "Deleted"

Scheduler 行为:
1. 返回: (true, false) - 暂时健康但不更新
2. 避免重复清理

何时设置为 "Deleted":
- CheckHealth 返回 (false, false) 时
- Scheduler 调用 NodeCleanUp 方法
```

**完整流程图**:

```
┌─────────────────────────────────────────────────────────────────┐
│ T=0s: Scheduler 定期检查（每 15 秒）                              │
│       发现注解为空或其他值                                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ T=0s: Scheduler 打上 "Requesting_2026-02-22 10:30:00"           │
│       返回 (true, true) - 触发设备信息更新                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ T=1s: Device Plugin 监听到注解变化                                │
│       重新上报设备信息，更新注解为 "Responded_2026-02-22 10:30:01"│
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ T=15s: Scheduler 再次检查                                        │
│        发现注解已更新（不再是 "Requesting"）                       │
│        返回 (true, true) - 触发设备信息更新                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 正常运行...                                                       │
└─────────────────────────────────────────────────────────────────┘

【异常情况：Device Plugin 崩溃】

┌─────────────────────────────────────────────────────────────────┐
│ T=0s: Scheduler 打上 "Requesting_2026-02-22 10:30:00"           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ T=1s: Device Plugin 崩溃，无响应                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ T=15s: Scheduler 检查，注解仍是 "Requesting_2026-02-22 10:30:00"│
│        未超时（15秒 < 60秒），返回 (true, false)                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ T=30s, T=45s: 继续等待...                                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ T=60s: Scheduler 检查，注解仍是 "Requesting_2026-02-22 10:30:00"│
│        超时！返回 (false, false) - 不健康                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ T=60s: Scheduler 调用 NodeCleanUp                                │
│        注解改为 "Deleted"                                         │
│        调用 rmNodeDevices 删除内存中的设备信息                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ T=75s: Scheduler 再次检查，发现注解为 "Deleted"                   │
│        返回 (true, false) - 跳过，避免重复清理                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ T=120s: Device Plugin 恢复，重新上报设备                          │
│         清除 "Deleted" 标记，注解改为正常值                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ T=135s: Scheduler 检查，发现设备恢复                              │
│         重新注册设备信息                                           │
└─────────────────────────────────────────────────────────────────┘
```

**代码示例**:
```go
func CheckHealth(devType string, node *corev1.Node) (bool, bool) {
    handshake := node.Annotations[util.HandshakeAnnos[devType]]
    
    if strings.Contains(handshake, "Requesting") {
        // 检查是否超时（60秒）
        formertime, _ := time.Parse(time.DateTime, strings.Split(handshake, "_")[1])
        return time.Now().Before(formertime.Add(time.Second * 60)), false
    } else if strings.Contains(handshake, "Deleted") {
        // 设备已下线，跳过
        return true, false
    } else {
        // 打上 "Requesting" 时间戳
        tmppat := make(map[string]string)
        tmppat[util.HandshakeAnnos[devType]] = "Requesting_" + time.Now().Format(time.DateTime)
        util.PatchNodeAnnotations(node, tmppat)
        return true, true
    }
}
```

**优点**:
- 可以主动触发 Device Plugin 重新上报设备信息
- 支持超时检测，及时发现 Device Plugin 故障
- 通过 "Deleted" 状态避免重复清理

**缺点**:
- 依赖注解通信，有一定延迟
- 需要 Device Plugin 配合实现握手逻辑
- 超时时间固定（60秒），不够灵活

### 3. AMD GPU - 简单返回健康

**实现位置**: `pkg/device/amd/device.go:127`

**工作原理**:
直接返回 `(true, true)`，不做任何健康检查。

**代码示例**:
```go
func (dev *AMDDevices) CheckHealth(devType string, n *corev1.Node) (bool, bool) {
    return true, true
}
```

**说明**:
- AMD 设备的健康检查由其他机制保证（如 Device Plugin 自身的健康检查）
- Scheduler 每次都会重新读取设备信息
- 适合设备状态变化频繁的场景

## NodeCleanUp 方法的作用

当 `CheckHealth` 返回 `(false, false)` 时，Scheduler 会调用 `NodeCleanUp` 方法清理节点。

### NVIDIA 的 NodeCleanUp

```go
func (dev *NvidiaGPUDevices) NodeCleanUp(nn string) error {
    return util.MarkAnnotationsToDelete(HandshakeAnnos, nn)
}
```

**疑问**: NVIDIA 的 CheckHealth 不使用 HandshakeAnnos，为什么 NodeCleanUp 要标记它？

**答案**:
1. **保持一致性**: 与其他设备类型保持统一的清理流程
2. **通知 Device Plugin**: 告知 Device Plugin 设备已被标记为不健康
3. **预留接口**: 为未来可能的握手机制预留接口
4. **无副作用**: 即使不使用握手机制，标记这个注解也不会有负面影响

### 清理流程

```
CheckHealth 返回 (false, false)
         │
         ▼
调用 NodeCleanUp(nodeName)
         │
         ▼
标记 HandshakeAnnos = "Deleted"
         │
         ▼
调用 rmNodeDevices(nodeName, deviceType)
         │
         ▼
从 Scheduler 内存中删除设备信息
         │
         ▼
后续调度不再使用该节点的设备
```

## Scheduler 的调用流程

### RegisterFromNodeAnnotations 循环

```go
func (s *Scheduler) RegisterFromNodeAnnotations() {
    ticker := time.NewTicker(time.Second * 15)
    for {
        select {
        case <-ticker.C:
            s.register(labelSelector, printedLog)
        }
    }
}
```

### register 方法

```go
func (s *Scheduler) register(labelSelector labels.Selector, printedLog map[string]bool) {
    rawNodes, _ := s.nodeLister.List(labelSelector)
    
    for _, node := range rawNodes {
        for devType, devInstance := range device.GetDevices() {
            // 1. 获取节点设备信息
            nodedevices, err := devInstance.GetNodeDevices(*node)
            if err != nil {
                continue
            }
            
            // 2. 调用设备的 CheckHealth 方法（接口方法，不是通用函数）
            health, needUpdate := devInstance.CheckHealth(devType, node)
            
            // 3. 如果不健康，清理节点
            if !health {
                devInstance.NodeCleanUp(node.Name)
                s.rmNodeDevices(node.Name, devType)
                continue
            }
            
            // 4. 如果需要更新，重新注册设备信息
            if needUpdate {
                nodeInfo := &device.NodeInfo{...}
                s.addNode(node.Name, nodeInfo)
            }
        }
    }
}
```

## 总结

| 设备类型 | 检查方式 | 优点 | 缺点 | 适用场景 |
|---------|---------|------|------|---------|
| NVIDIA GPU | 设备数量对比 | 简单直接，实时性好 | 只能检测数量变化 | 设备数量稳定 |
| Hygon DCU | HandshakeAnnos 握手 | 可主动触发更新，支持超时检测 | 有延迟，需要 Device Plugin 配合 | 需要主动同步 |
| AMD GPU | 始终返回健康 | 无额外开销 | 无法检测故障 | 依赖其他机制 |

## 常见问题

### Q1: 为什么 NVIDIA 不使用 HandshakeAnnos？

**A**: NVIDIA 的健康检查基于 `Status.Allocatable` 数量对比，这是 Kubernetes 原生机制，更简单直接。HandshakeAnnos 是为需要主动触发更新的设备类型设计的。

### Q2: CheckHealth 返回的两个 bool 值分别是什么意思？

**A**:
- 第一个 bool: 设备是否健康
  - `false`: 触发 `NodeCleanUp` 和 `rmNodeDevices`
  - `true`: 设备正常
- 第二个 bool: 是否需要更新设备信息
  - `true`: 重新调用 `GetNodeDevices` 读取设备信息
  - `false`: 跳过更新

### Q3: 为什么 "Deleted" 状态返回 `(true, false)`？

**A**: 
- 返回 `true` 避免重复调用 `NodeCleanUp`（已经清理过了）
- 返回 `false` 避免重复更新设备信息（设备已下线，没有信息可更新）
- 等待 Device Plugin 恢复后重新注册

### Q4: 超时时间为什么是 60 秒？

**A**: 这是一个经验值，平衡了以下因素：
- Device Plugin 重启通常在 30 秒内完成
- 给予足够的缓冲时间避免误判
- 不能太长，否则影响故障检测的及时性

### Q5: 如果 Device Plugin 恢复了，如何重新注册设备？

**A**:
1. Device Plugin 恢复后，会重新上报设备信息到节点注解
2. Device Plugin 清除 "Deleted" 标记，更新 HandshakeAnnos
3. Scheduler 下次检查时发现注解变化，重新读取设备信息
4. 设备重新加入调度池

## 相关代码位置

- NVIDIA CheckHealth: `pkg/device/nvidia/device.go:223`
- NVIDIA NodeCleanUp: `pkg/device/nvidia/device.go:220`
- 通用 CheckHealth: `pkg/device/devices.go:957`
- Hygon CheckHealth: `pkg/device/hygon/device.go:183`
- AMD CheckHealth: `pkg/device/amd/device.go:127`
- Scheduler register: `pkg/scheduler/scheduler.go:414`
- rmNodeDevices: `pkg/scheduler/nodes.go:115`

