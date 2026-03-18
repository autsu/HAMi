# 设备健康检查与调度流程详解

## 问题 1: `rmNodeDevices` 删除后会发生什么？调度时会跳过这个节点吗？

### 删除流程

当 `CheckHealth` 返回 `(false, false)` 时，会触发以下清理流程：

```go
// pkg/scheduler/scheduler.go:455
if !health {
    klog.Warning("Device is unhealthy, cleaning up node", "nodeName", val.Name, "deviceVendor", devhandsk)
    err := devInstance.NodeCleanUp(val.Name)
    if err != nil {
        klog.ErrorS(err, "Node cleanup failed", "nodeName", val.Name, "deviceVendor", devhandsk)
    }
    
    // 从 Scheduler 内存中删除节点上对应的设备
    s.rmNodeDevices(val.Name, devhandsk)
    continue
}
```

### `rmNodeDevices` 的作用

```go
// pkg/scheduler/nodes.go:115
func (m *nodeManager) rmNodeDevices(nodeID string, deviceVendor string) {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    nodeInfo := m.nodes[nodeID]
    if nodeInfo == nil {
        return
    }
    // 删除节点上的特定设备类型
    delete(m.nodes[nodeID].Devices, deviceVendor)
    
    // 如果节点没有任何设备了，删除整个节点
    if len(m.nodes[nodeID].Devices) == 0 {
        delete(m.nodes, nodeID)
    }
    klog.InfoS("Removing device from node", "nodeName", nodeID, "deviceVendor", deviceVendor)
}
```

### 调度时会发生什么？

#### 情况 1: 节点只有一种设备类型（如只有 NVIDIA GPU）

```
删除前：
nodes["node1"] = {
    Devices: {
        "NVIDIA": [GPU-0, GPU-1, GPU-2, ...]
    }
}

删除后：
nodes["node1"] = 不存在（整个节点被删除）
```

**调度行为**：
- ✅ **会跳过这个节点**
- 原因：`GetNode("node1")` 返回错误 `"node node1 not found"`
- 在 `getNodesUsage` 中被标记为 `"node unregistered"`

```go
// pkg/scheduler/scheduler.go:570
for _, nodeID := range *nodes {
    node, err := s.GetNode(nodeID)
    if err != nil {
        klog.V(5).InfoS("node unregistered", "node", nodeID, "error", err)
        failedNodes[nodeID] = "node unregistered"
        continue  // 跳过这个节点
    }
    cachenodeMap[node.ID] = overallnodeMap[node.ID]
}
```

#### 情况 2: 节点有多种设备类型（异构设备，如 NVIDIA GPU + AMD GPU）

```
删除前：
nodeManager.nodes["node1"] = {
    Devices: {
        "NVIDIA": [GPU-0, GPU-1, ...],
        "AMD": [GPU-0, GPU-1, ...]
    }
}

删除 NVIDIA 后：
nodeManager.nodes["node1"] = {
    Devices: {
        "AMD": [GPU-0, GPU-1, ...]  // NVIDIA 被删除，AMD 保留
    }
}
```

**关键点**：
- ✅ **节点仍然存在于 `nodeManager.nodes` 中**
- ✅ **`GetNode("node1")` 返回成功**（因为节点还有 AMD 设备）
- ✅ **节点会被加入 `cachenodeMap`**（不会被标记为 "node unregistered"）

**调度行为详解**：

##### 2.1 请求 NVIDIA GPU 的 Pod

```go
// Pod 请求: nvidia.com/gpu=1

// getNodesUsage 阶段:
node, err := s.GetNode("node1")  // ✅ 成功，节点存在
cachenodeMap["node1"] = overallnodeMap["node1"]  // ✅ 加入候选

// overallnodeMap["node1"] 的内容:
{
    Node: <node1 对象>,
    Devices: {
        DeviceLists: [
            // 只有 AMD 设备，没有 NVIDIA 设备
            {Device: {Type: "AMD", ID: "AMD-GPU-0", ...}},
            {Device: {Type: "AMD", ID: "AMD-GPU-1", ...}},
        ]
    }
}

// calcScore 阶段（真实代码逻辑）:
// pkg/scheduler/score.go:calcScore() 调用 fitInDevices()

// 1. getNodeResources() 过滤设备类型
func getNodeResources(list NodeUsage, t string) []*device.DeviceUsage {
    l := []*device.DeviceUsage{}
    for _, val := range list.Devices.DeviceLists {
        if strings.Contains(val.Device.Type, t) {  // 匹配 "NVIDIA"
            l = append(l, val.Device)
        }
    }
    return l  // ❌ 返回空列表（node1 没有 NVIDIA 设备）
}

// 2. 调用设备的 Fit 方法
fit, tmpDevs, reason := device.GetDevices()["NVIDIA"].Fit(
    getNodeResources(*node, "NVIDIA"),  // ❌ 传入空列表
    request,
    pod,
    nodeInfo,
    devinput
)
// 结果: fit = false, reason = "insufficient NVIDIA devices"

// 3. 标记节点失败
failedNodes["node1"] = "node unfit pod"
failureReason["insufficient NVIDIA devices"] = append(..., "node1")
```

**结论**：
- ❌ **不能调度使用 NVIDIA GPU 的 Pod**
- 原因：节点的 `DeviceLists` 中没有 NVIDIA 设备
- 行为：在 `calcScore` 阶段被过滤，不是在 `getNodesUsage` 阶段

##### 2.2 请求 AMD GPU 的 Pod

```go
// Pod 请求: amd.com/gpu=1

// getNodesUsage 阶段:
node, err := s.GetNode("node1")  // ✅ 成功
cachenodeMap["node1"] = overallnodeMap["node1"]  // ✅ 加入候选

// calcScore 阶段（真实代码逻辑）:

// 1. getNodeResources() 过滤设备类型
func getNodeResources(list NodeUsage, t string) []*device.DeviceUsage {
    l := []*device.DeviceUsage{}
    for _, val := range list.Devices.DeviceLists {
        if strings.Contains(val.Device.Type, t) {  // 匹配 "AMD"
            l = append(l, val.Device)
        }
    }
    return l  // ✅ 返回 AMD 设备列表
}

// 2. 调用设备的 Fit 方法
fit, tmpDevs, reason := device.GetDevices()["AMD"].Fit(
    getNodeResources(*node, "AMD"),  // ✅ 传入 AMD 设备列表
    request,
    pod,
    nodeInfo,
    devinput
)
// 结果: fit = true, tmpDevs 包含分配的 AMD 设备

// 3. 更新设备使用量
for _, val := range tmpDevs["AMD"] {
    device.GetDevices()["AMD"].AddResourceUsage(pod, deviceUsage, &val)
}
// 结果: 分配成功
```

**结论**：
- ✅ **可以调度使用 AMD GPU 的 Pod**
- 原因：节点的 `DeviceLists` 中有 AMD 设备
- 行为：正常通过 `calcScore`，成功调度

### 完整的调度流程（单设备节点 vs 异构设备节点）

#### 场景 A: 单设备节点（只有 NVIDIA GPU）

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Kubernetes 调度器调用 Filter                                   │
│    Pod 请求: nvidia.com/gpu=1                                    │
│    候选节点: ["node1", "node2", "node3"]                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. getNodesUsage() - 构建节点设备视图                              │
│    遍历候选节点，调用 GetNode(nodeID)                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. 处理 node1（NVIDIA 设备不健康，已被 rmNodeDevices 删除）        │
│                                                                   │
│    nodeManager.nodes["node1"] = 不存在（整个节点被删除）           │
│                                                                   │
│    node, err := s.GetNode("node1")                               │
│    // err = "node node1 not found"                               │
│                                                                   │
│    failedNodes["node1"] = "node unregistered"                    │
│    // ❌ node1 不会加入 cachenodeMap                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. calcScore() - 计算节点分数                                      │
│    只对 cachenodeMap 中的节点打分                                  │
│    node1 不在 cachenodeMap 中，直接跳过                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. 返回结果                                                        │
│    NodeNames: ["node2"] 或 ["node3"]                             │
│    FailedNodes: {"node1": "node unregistered"}                   │
└─────────────────────────────────────────────────────────────────┘
```

#### 场景 B: 异构设备节点（NVIDIA GPU + AMD GPU）

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Kubernetes 调度器调用 Filter                                   │
│    Pod 请求: nvidia.com/gpu=1                                    │
│    候选节点: ["node1", "node2", "node3"]                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. getNodesUsage() - 构建节点设备视图                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. 处理 node1（NVIDIA 不健康，AMD 健康）                           │
│                                                                   │
│    nodeManager.nodes["node1"] = {                                │
│        Devices: {                                                │
│            "AMD": [GPU-0, GPU-1, ...]  // 只有 AMD，NVIDIA 被删除 │
│        }                                                          │
│    }                                                              │
│                                                                   │
│    node, err := s.GetNode("node1")                               │
│    // ✅ err = nil，节点存在（因为还有 AMD 设备）                   │
│                                                                   │
│    // ✅ node1 被加入 cachenodeMap                                │
│    cachenodeMap["node1"] = overallnodeMap["node1"]               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. ListNodes() 返回 node1 的设备信息                               │
│                                                                   │
│    allNodes["node1"] = {                                          │
│        Devices: {                                                │
│            "AMD": [                                              │
│                {ID: "AMD-GPU-0", Type: "AMD", ...},              │
│                {ID: "AMD-GPU-1", Type: "AMD", ...}               │
│            ]                                                      │
│            // 注意：没有 "NVIDIA" 键                              │
│        }                                                          │
│    }                                                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. 构建 overallnodeMap["node1"]                                   │
│                                                                   │
│    for _, k := range node.Devices {  // 只遍历 AMD                │
│        for _, d := range k {                                      │
│            nodeInfo.Devices.DeviceLists = append(..., &Device{   │
│                Type: "AMD",  // 只有 AMD 设备                     │
│                ID: "AMD-GPU-0",                                   │
│                ...                                                │
│            })                                                     │
│        }                                                          │
│    }                                                              │
│                                                                   │
│    // 结果: DeviceLists 中只有 AMD 设备，没有 NVIDIA 设备         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. calcScore() - 尝试为 Pod 分配 NVIDIA 设备                       │
│                                                                   │
│    // 调用 getNodeResources() 过滤 NVIDIA 设备                    │
│    nvidiaDevices := getNodeResources(*node1, "NVIDIA")           │
│    // 返回: []  (空列表，node1 没有 NVIDIA 设备)                   │
│                                                                   │
│    // 调用 NVIDIA 设备的 Fit 方法                                  │
│    fit, tmpDevs, reason := device.GetDevices()["NVIDIA"].Fit(    │
│        nvidiaDevices,  // 传入空列表                              │
│        request, pod, nodeInfo, devinput                           │
│    )                                                              │
│    // 返回: fit=false, reason="insufficient NVIDIA devices"       │
│                                                                   │
│    // 标记节点失败                                                 │
│    failedNodes["node1"] = "node unfit pod"                       │
│    // ❌ node1 在 calcScore 阶段被过滤                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 7. 返回结果                                                        │
│    NodeNames: ["node2"] 或 ["node3"]                             │
│    FailedNodes: {                                                │
│        "node1": "insufficient NVIDIA devices"                    │
│    }                                                              │
└─────────────────────────────────────────────────────────────────┘
```

#### 场景 C: 异构设备节点 + 请求 AMD GPU

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Kubernetes 调度器调用 Filter                                   │
│    Pod 请求: amd.com/gpu=1  // 注意：请求 AMD，不是 NVIDIA        │
│    候选节点: ["node1", "node2", "node3"]                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2-5. getNodesUsage() 阶段（同场景 B）                              │
│      node1 被加入 cachenodeMap                                    │
│      DeviceLists 中只有 AMD 设备                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. calcScore() - 尝试为 Pod 分配 AMD 设备                          │
│                                                                   │
│    for _, device := range node1.Devices.DeviceLists {            │
│        if device.Type == "AMD" {  // ✅ 找到匹配！                 │
│            if device.Totalmem - device.Usedmem >= request.Memreq {│
│                // ✅ 资源充足，分配成功                            │
│                allocated = append(allocated, device)              │
│                return true  // 分配成功                           │
│            }                                                      │
│        }                                                          │
│    }                                                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 7. 返回结果                                                        │
│    NodeNames: ["node1"]  // ✅ node1 被选中！                     │
│    FailedNodes: {}                                               │
└─────────────────────────────────────────────────────────────────┘
```

### 实际例子

#### 例子 1: 单设备节点

假设集群有 3 个节点：
- node1: 8 个 NVIDIA GPU（健康）
- node2: 8 个 NVIDIA GPU（不健康，被删除）
- node3: 8 个 NVIDIA GPU（健康）

当 Pod 请求 1 个 NVIDIA GPU 时：

```
Filter 输入:
  NodeNames: ["node1", "node2", "node3"]

getNodesUsage 处理:
  node1: ✅ GetNode 成功 → 加入 cachenodeMap
  node2: ❌ GetNode 失败 → failedNodes["node2"] = "node unregistered"
  node3: ✅ GetNode 成功 → 加入 cachenodeMap

calcScore 处理:
  只对 node1 和 node3 打分
  node2 不在候选列表中

Filter 输出:
  NodeNames: ["node1"] 或 ["node3"]（选择分数最高的）
  FailedNodes: {"node2": "node unregistered"}
```

#### 例子 2: 异构设备节点

假设集群有 3 个节点：
- node1: 4 个 NVIDIA GPU（健康）+ 4 个 AMD GPU（健康）
- node2: 4 个 NVIDIA GPU（不健康，被删除）+ 4 个 AMD GPU（健康）
- node3: 4 个 NVIDIA GPU（健康）+ 4 个 AMD GPU（健康）

##### 场景 2.1: Pod 请求 1 个 NVIDIA GPU

```
Filter 输入:
  NodeNames: ["node1", "node2", "node3"]

getNodesUsage 处理:
  node1: ✅ GetNode 成功 → 加入 cachenodeMap
         DeviceLists: [NVIDIA-GPU-0, NVIDIA-GPU-1, ..., AMD-GPU-0, AMD-GPU-1, ...]
  
  node2: ✅ GetNode 成功 → 加入 cachenodeMap（节点还有 AMD 设备）
         DeviceLists: [AMD-GPU-0, AMD-GPU-1, ...]  // 只有 AMD，没有 NVIDIA
  
  node3: ✅ GetNode 成功 → 加入 cachenodeMap
         DeviceLists: [NVIDIA-GPU-0, NVIDIA-GPU-1, ..., AMD-GPU-0, AMD-GPU-1, ...]

calcScore 处理:
  node1: ✅ 找到 NVIDIA 设备，分配成功
  node2: ❌ 没有 NVIDIA 设备 → failedNodes["node2"] = "insufficient NVIDIA devices"
  node3: ✅ 找到 NVIDIA 设备，分配成功

Filter 输出:
  NodeNames: ["node1"] 或 ["node3"]（选择分数最高的）
  FailedNodes: {"node2": "insufficient NVIDIA devices"}
```

##### 场景 2.2: Pod 请求 1 个 AMD GPU

```
Filter 输入:
  NodeNames: ["node1", "node2", "node3"]

getNodesUsage 处理:
  node1: ✅ GetNode 成功 → 加入 cachenodeMap
  node2: ✅ GetNode 成功 → 加入 cachenodeMap
  node3: ✅ GetNode 成功 → 加入 cachenodeMap

calcScore 处理:
  node1: ✅ 找到 AMD 设备，分配成功
  node2: ✅ 找到 AMD 设备，分配成功（虽然 NVIDIA 不健康，但 AMD 可用）
  node3: ✅ 找到 AMD 设备，分配成功

Filter 输出:
  NodeNames: ["node1"] 或 ["node2"] 或 ["node3"]（选择分数最高的）
  FailedNodes: {}
```

### 关键区别：单设备节点 vs 异构设备节点

| 对比项 | 单设备节点 | 异构设备节点 |
|--------|-----------|-------------|
| **节点配置** | 只有 NVIDIA GPU | NVIDIA GPU + AMD GPU |
| **NVIDIA 不健康后** | 整个节点从 `nodeManager.nodes` 删除 | 节点保留，只删除 NVIDIA 设备 |
| **`GetNode("node1")` 结果** | ❌ 返回错误 `"node not found"` | ✅ 返回成功（节点还有 AMD） |
| **是否加入 `cachenodeMap`** | ❌ 不加入，标记为 "node unregistered" | ✅ 加入候选节点列表 |
| **`overallnodeMap["node1"]` 内容** | 不存在 | 只包含 AMD 设备的 `DeviceLists` |
| **请求 NVIDIA GPU 的 Pod** | 在 `getNodesUsage` 阶段被过滤 | 在 `calcScore` 阶段被过滤 |
| **请求 AMD GPU 的 Pod** | 无法调度（节点不存在） | ✅ 可以正常调度 |
| **过滤阶段** | `getNodesUsage` | `calcScore` |
| **失败原因** | `"node unregistered"` | `"insufficient NVIDIA devices"` |

### 总结

| 场景 | 节点状态 | `GetNode()` 结果 | 加入 `cachenodeMap` | 调度行为 |
|------|---------|-----------------|-------------------|---------|
| **单设备类型节点**<br>设备不健康 | 整个节点被删除 | ❌ 返回错误 | ❌ 不加入 | 在 `getNodesUsage` 阶段跳过<br>失败原因: "node unregistered" |
| **异构设备节点**<br>NVIDIA 不健康<br>请求 NVIDIA GPU | 只删除 NVIDIA 设备<br>节点保留 | ✅ 返回成功 | ✅ 加入 | 在 `calcScore` 阶段被过滤<br>失败原因: "insufficient NVIDIA devices" |
| **异构设备节点**<br>NVIDIA 不健康<br>请求 AMD GPU | 只删除 NVIDIA 设备<br>节点保留 | ✅ 返回成功 | ✅ 加入 | ✅ 正常调度到该节点<br>使用 AMD 设备 |
| **设备恢复后** | Device Plugin 重新上报<br>节点重新注册 | ✅ 返回成功 | ✅ 加入 | ✅ 恢复正常调度 |

---

## 问题 2: Device Plugin 是如何检测设备不健康的？

### Device Plugin 的健康检查机制

Device Plugin 使用 **NVML (NVIDIA Management Library)** 进行实时的硬件级健康检查。

### 核心实现

```go
// pkg/device-plugin/nvidiadevice/nvinternal/rm/health.go:48
func (r *nvmlResourceManager) checkHealth(
    stop <-chan interface{}, 
    devices Devices, 
    unhealthy chan<- *Device, 
    disableNVML <-chan bool
) error {
    // 1. 初始化 NVML
    ret := r.nvml.Init()
    if ret != nvml.SUCCESS {
        return fmt.Errorf("failed to initialize NVML: %v", ret)
    }
    defer r.nvml.Shutdown()

    // 2. 创建事件集（用于监听 GPU 事件）
    eventSet, ret := r.nvml.EventSetCreate()
    if ret != nvml.SUCCESS {
        return fmt.Errorf("failed to create event set: %v", ret)
    }
    defer eventSet.Free()

    // 3. 为每个设备注册事件监听
    eventMask := uint64(
        nvml.EventTypeXidCriticalError |      // 严重错误
        nvml.EventTypeDoubleBitEccError |     // 双位 ECC 错误
        nvml.EventTypeSingleBitEccError       // 单位 ECC 错误
    )
    
    for _, d := range devices {
        gpu, ret := r.nvml.DeviceGetHandleByUUID(d.UUID)
        if ret != nvml.SUCCESS {
            unhealthy <- d  // 无法获取设备句柄，标记为不健康
            continue
        }
        
        // 注册事件监听
        ret = gpu.RegisterEvents(eventMask, eventSet)
        if ret != nvml.SUCCESS {
            unhealthy <- d  // 注册失败，标记为不健康
        }
    }

    // 4. 持续监听事件
    for {
        select {
        case <-stop:
            return nil
        default:
        }

        // 等待事件（超时 5 秒）
        e, ret := eventSet.Wait(5000)
        if ret == nvml.ERROR_TIMEOUT {
            continue  // 超时，继续等待
        }
        if ret != nvml.SUCCESS {
            // 等待失败，标记所有设备为不健康
            for _, d := range devices {
                unhealthy <- d
            }
            continue
        }

        // 5. 处理 XID 严重错误
        if e.EventType == nvml.EventTypeXidCriticalError {
            // 检查是否是被忽略的 XID
            if !xids.IsDisabled(e.EventData) {
                klog.Infof("XidCriticalError: Xid=%d on Device=%s; marking device as unhealthy.", 
                    e.EventData, d.ID)
                unhealthy <- d  // 标记设备为不健康
            }
        }
    }
}
```

### 监听的事件类型

Device Plugin 监听以下三种 GPU 事件：

#### 1. **XID 严重错误** (`EventTypeXidCriticalError`)

XID (X-Window System ID) 是 NVIDIA 驱动报告的硬件错误代码。

**常见的致命 XID**：
- XID 48: Double Bit ECC Error（双位 ECC 错误，无法纠正）
- XID 63: Row Remapper Error（行重映射错误）
- XID 64: Uncorrectable ECC Error（不可纠正的 ECC 错误）
- XID 74: NVLink Error（NVLink 连接错误）
- XID 79: GPU Fallen Off the Bus（GPU 从总线掉落）
- XID 94: Contained ECC Error（包含的 ECC 错误）
- XID 95: Uncontained ECC Error（未包含的 ECC 错误）

**被忽略的 XID**（应用层错误，不影响硬件健康）：
```go
ignoredXids := []uint64{
    13,  // Graphics Engine Exception（图形引擎异常）
    31,  // GPU memory page fault（GPU 内存页错误）
    43,  // GPU stopped processing（GPU 停止处理）
    45,  // Preemptive cleanup（抢占式清理）
    68,  // Video processor exception（视频处理器异常）
    109, // Context Switch Timeout Error（上下文切换超时）
}
```

#### 2. **双位 ECC 错误** (`EventTypeDoubleBitEccError`)

- ECC (Error-Correcting Code) 内存错误
- 单位错误可以自动纠正
- 双位错误无法纠正，表示硬件故障

#### 3. **单位 ECC 错误** (`EventTypeSingleBitEccError`)

- 可以自动纠正的内存错误
- 频繁出现可能预示硬件问题

### 健康检查流程图

```
┌─────────────────────────────────────────────────────────────────┐
│ Device Plugin 启动                                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. 初始化 NVML                                                    │
│    ret := nvml.Init()                                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. 创建事件集                                                      │
│    eventSet := nvml.EventSetCreate()                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. 为每个 GPU 注册事件监听                                          │
│    gpu.RegisterEvents(XidCriticalError | EccError, eventSet)    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. 持续监听事件（阻塞等待）                                          │
│    event := eventSet.Wait(5000ms)                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────┴─────────┐
                    │                   │
                    ▼                   ▼
        ┌───────────────────┐   ┌───────────────────┐
        │ 超时（5秒）        │   │ 收到事件           │
        │ 继续等待           │   │ 处理事件           │
        └───────────────────┘   └───────────────────┘
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │ 是 XID 严重错误？      │
                              └───────────────────────┘
                                          │
                                ┌─────────┴─────────┐
                                │                   │
                                ▼                   ▼
                        ┌───────────────┐   ┌───────────────┐
                        │ 是            │   │ 否            │
                        │ 检查 XID 类型  │   │ 忽略          │
                        └───────────────┘   └───────────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
                    ▼                       ▼
        ┌───────────────────┐   ┌───────────────────┐
        │ 致命 XID          │   │ 可忽略 XID        │
        │ (48, 63, 64...)   │   │ (13, 31, 43...)   │
        └───────────────────┘   └───────────────────┘
                    │                       │
                    ▼                       ▼
        ┌───────────────────┐   ┌───────────────────┐
        │ unhealthy <- device│   │ 忽略，继续监听     │
        └───────────────────┘   └───────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. ListAndWatch 收到 unhealthy 通知                               │
│    标记设备为 Unhealthy，通知 kubelet                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. kubelet 更新节点的 Status.Allocatable                          │
│    nvidia.com/gpu: 7 (从 8 减少到 7)                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 7. Scheduler 的 CheckHealth 检测到设备数量变化                     │
│    current=7, reported=8 → 返回 (false, false)                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 8. Scheduler 调用 rmNodeDevices 删除节点设备信息                   │
└─────────────────────────────────────────────────────────────────┘
```

### Device Plugin 与 Scheduler 的协作

```
┌──────────────────────┐                    ┌──────────────────────┐
│   Device Plugin      │                    │     Scheduler        │
│  (硬件级健康检查)     │                    │  (数量级健康检查)     │
└──────────────────────┘                    └──────────────────────┘
          │                                            │
          │ 1. NVML 监听 GPU 事件                      │
          │    (XID 错误、ECC 错误)                    │
          │                                            │
          ▼                                            │
    检测到 XID 48                                      │
    (双位 ECC 错误)                                    │
          │                                            │
          │ 2. 标记设备为 Unhealthy                    │
          │    unhealthy <- device                     │
          │                                            │
          ▼                                            │
    ListAndWatch 发送更新                              │
    Device.Health = Unhealthy                          │
          │                                            │
          │ 3. 通知 kubelet                            │
          │                                            │
          ▼                                            │
    kubelet 更新节点状态                               │
    Status.Allocatable:                                │
      nvidia.com/gpu: 7 (从 8 减少)                    │
          │                                            │
          │                                            │ 4. 定期检查（每 15 秒）
          │                                            │    读取 Status.Allocatable
          │                                            │
          │                                            ▼
          │                                      current = 7
          │                                      reported = 8
          │                                            │
          │                                            │ 5. 检测到数量变化
          │                                            │    返回 (false, false)
          │                                            │
          │                                            ▼
          │                                      调用 NodeCleanUp
          │                                      调用 rmNodeDevices
          │                                            │
          │                                            ▼
          │                                      从内存中删除设备信息
          │                                      后续调度跳过该节点
```

### 为什么需要两层健康检查？

| 层级 | 检查者 | 检查方式 | 检查频率 | 检测内容 | 优点 | 缺点 |
|------|--------|---------|---------|---------|------|------|
| **硬件层** | Device Plugin | NVML 事件监听 | 实时（事件驱动） | XID 错误、ECC 错误、温度、功耗 | 实时性好，检测精准 | 只能检测硬件故障 |
| **数量层** | Scheduler | Status.Allocatable 对比 | 定期（15 秒） | 设备数量变化 | 简单可靠，检测 Device Plugin 崩溃 | 延迟较高，无法检测硬件细节 |

### 实际例子

#### 场景 1: GPU 硬件故障

```
T=0s:   GPU-0 发生双位 ECC 错误（XID 48）
T=0.1s: Device Plugin 的 NVML 监听到事件
T=0.1s: Device Plugin 标记 GPU-0 为 Unhealthy
T=0.2s: kubelet 收到通知，更新 Status.Allocatable: nvidia.com/gpu=7
T=15s:  Scheduler 检查，发现 current=7, reported=8
T=15s:  Scheduler 调用 rmNodeDevices，删除节点设备信息
T=15s:  后续调度跳过该节点
```

#### 场景 2: Device Plugin 崩溃

```
T=0s:   Device Plugin 进程崩溃
T=0s:   kubelet 检测到 Device Plugin socket 断开
T=1s:   kubelet 更新 Status.Allocatable: nvidia.com/gpu=0
T=15s:  Scheduler 检查，发现 current=0, reported=8
T=15s:  Scheduler 返回 (false, false) - 不健康
T=15s:  Scheduler 调用 rmNodeDevices，删除节点设备信息
T=60s:  Device Plugin 重启，重新注册设备
T=75s:  Scheduler 检查，发现 current=8, reported=0
T=75s:  Scheduler 返回 (true, true) - 需要更新
T=75s:  Scheduler 重新注册节点设备信息
```

### 总结

1. **Device Plugin 的健康检查**：
   - 使用 NVML 实时监听 GPU 硬件事件
   - 检测 XID 错误、ECC 错误等硬件故障
   - 通过 `unhealthy` 通道通知 kubelet
   - kubelet 更新 `Status.Allocatable` 中的设备数量

2. **Scheduler 的健康检查**：
   - 定期（每 15 秒）检查 `Status.Allocatable` 中的设备数量
   - 与上次记录的数量对比
   - 检测到数量变化或设备消失时，调用 `rmNodeDevices`
   - 从内存中删除设备信息，后续调度跳过该节点

3. **两层检查的协作**：
   - Device Plugin 负责硬件级的实时检测
   - Scheduler 负责数量级的定期检测
   - 互为补充，确保设备健康状态的准确性

