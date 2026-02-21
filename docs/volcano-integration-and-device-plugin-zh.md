# HAMi 与 Volcano 集成及 Device Plugin 实现详解

## 问题 1：HAMi 如何与 Volcano 集成？

### 答案：通过独立的 Device Plugin 项目，而非硬编码

HAMi 与 Volcano 的集成**不是**通过硬编码到 Volcano 代码中实现的，而是通过一个**独立的项目**：

**[volcano-vgpu-device-plugin](https://github.com/Project-HAMi/volcano-vgpu-device-plugin)**

### 集成架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│                                                              │
│  ┌──────────────────┐         ┌──────────────────┐         │
│  │  Volcano         │         │  HAMi (默认)      │         │
│  │  Scheduler       │         │  Scheduler       │         │
│  │                  │         │  Extender        │         │
│  │  - Gang 调度     │         │  - 设备调度      │         │
│  │  - 队列管理      │         │  - 细粒度分配    │         │
│  │  - 优先级        │         │                  │         │
│  └────────┬─────────┘         └──────────────────┘         │
│           │                                                  │
│           │ 调用                                             │
│           ↓                                                  │
│  ┌──────────────────────────────────────────────┐          │
│  │  Volcano DeviceShare Plugin                   │          │
│  │  (内置在 Volcano Scheduler 中)                │          │
│  │                                               │          │
│  │  - 读取 volcano.sh/vgpu-* 资源               │          │
│  │  - 调用设备分配逻辑                          │          │
│  └────────┬─────────────────────────────────────┘          │
│           │                                                  │
│           │ 通过 Node 注解通信                              │
│           ↓                                                  │
│  ┌──────────────────────────────────────────────┐          │
│  │  volcano-vgpu-device-plugin (DaemonSet)      │          │
│  │  (独立项目，复用 HAMi-core)                  │          │
│  │                                               │          │
│  │  - 上报 volcano.sh/vgpu-number               │          │
│  │  - 实现 Allocate 方法                        │          │
│  │  - 使用 HAMi-core 做资源隔离                 │          │
│  └──────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

### 关键点

1. **HAMi 本身不依赖 Volcano**
   - `go.mod` 中没有 Volcano 依赖
   - HAMi 默认使用 kube-scheduler extender

2. **Volcano 通过插件机制扩展**
   - Volcano 内置 `deviceshare` 插件
   - 插件读取特定的资源名称：`volcano.sh/vgpu-number`、`volcano.sh/vgpu-memory`

3. **volcano-vgpu-device-plugin 是桥梁**
   - 独立的 Device Plugin 项目
   - 复用 HAMi-core 的资源隔离能力
   - 上报 Volcano 识别的资源名称

### Volcano 配置示例

```yaml
# Volcano Scheduler 配置
apiVersion: v1
kind: ConfigMap
metadata:
  name: volcano-scheduler-configmap
  namespace: volcano-system
data:
  volcano-scheduler.conf: |
    actions: "enqueue, allocate, backfill"
    tiers:
    - plugins:
      - name: gang          # Gang 调度插件（Volcano 内置）
      - name: conformance
    - plugins:
      - name: deviceshare   # 设备共享插件（Volcano 内置）
        arguments:
          deviceshare.VGPUEnable: true  # 启用 vGPU 支持
```

**deviceshare 插件的工作原理**：
1. 读取 Pod 的资源请求：`volcano.sh/vgpu-number`、`volcano.sh/vgpu-memory`
2. 查找有足够资源的节点
3. 调用 Volcano 的设备分配算法
4. 将分配结果写入 Pod 注解

### 资源名称对比

| 调度器 | Device Plugin | 资源名称 | 注解前缀 |
|--------|--------------|----------|----------|
| **kube-scheduler + HAMi** | HAMi Device Plugin | `nvidia.com/gpu`<br>`nvidia.com/gpumem`<br>`nvidia.com/gpucores` | `hami.io/` |
| **Volcano + volcano-vgpu** | volcano-vgpu-device-plugin | `volcano.sh/vgpu-number`<br>`volcano.sh/vgpu-memory`<br>`volcano.sh/vgpu-cores` | `volcano.sh/` |

### 为什么这样设计？

**优点**：
1. **解耦**：HAMi 和 Volcano 独立开发，互不依赖
2. **灵活**：用户可以选择使用哪个调度器
3. **复用**：volcano-vgpu-device-plugin 复用 HAMi-core 的隔离能力
4. **标准化**：都遵循 Kubernetes Device Plugin API

**缺点**：
1. **维护成本**：需要维护两套 Device Plugin
2. **功能差异**：两个 Device Plugin 的功能可能不完全一致

---

## 问题 2：为什么要自己实现 Device Plugin？

### 答案：基于 NVIDIA 官方插件修改，增加细粒度共享能力

从代码头部的注释可以看出：

```go
/*
 * Licensed to NVIDIA CORPORATION under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. NVIDIA CORPORATION licenses this file to you under
 * the Apache License, Version 2.0 (the "License");
 */

/*
 * Modifications Copyright The HAMi Authors. See
 * GitHub history for details.
 */
```

**HAMi 的 Device Plugin 是基于 NVIDIA 官方 Device Plugin 修改的**。

### NVIDIA 官方 Device Plugin 的限制

NVIDIA 官方的 [k8s-device-plugin](https://github.com/NVIDIA/k8s-device-plugin) 只支持：

1. **整卡分配**：一个 Pod 只能请求整数个 GPU
2. **Time-Slicing**：通过配置可以虚拟出多个 GPU，但没有资源隔离
3. **MIG 模式**：支持 MIG 切分，但粒度固定

**不支持**：
- ❌ 细粒度显存分配（如 2GB、4GB）
- ❌ 细粒度算力分配（如 50%、80%）
- ❌ 硬隔离（防止容器超用资源）

### HAMi Device Plugin 的增强

HAMi 在 NVIDIA 官方插件基础上增加了：

#### 1. 细粒度资源请求

```yaml
# NVIDIA 官方插件：只能请求整卡
resources:
  limits:
    nvidia.com/gpu: 1  # 只能是整数

# HAMi Device Plugin：可以请求部分资源
resources:
  limits:
    nvidia.com/gpu: 1           # 物理 GPU 数量
    nvidia.com/gpumem: 4000     # 4GB 显存
    nvidia.com/gpucores: 50     # 50% 算力
```

#### 2. 硬隔离机制

**关键代码**（`server.go:665-685`）：

```go
// HAMi 特有的资源隔离逻辑
if plugin.operatingMode != "mig" {
    for i, dev := range devreq {
        // 设置每个 GPU 的显存限制
        limitKey := fmt.Sprintf("CUDA_DEVICE_MEMORY_LIMIT_%v", i)
        response.Envs[limitKey] = fmt.Sprintf("%vm", dev.Usedmem)
    }
    // 设置算力限制
    response.Envs["CUDA_DEVICE_SM_LIMIT"] = fmt.Sprint(devreq[0].Usedcores)
    
    // 设置缓存文件路径（用于资源隔离）
    response.Envs["CUDA_DEVICE_MEMORY_SHARED_CACHE"] = fmt.Sprintf("%s/vgpu/%v.cache", hostHookPath, uuid.New().String())
    
    // 挂载 vGPU 库（拦截 CUDA API）
    response.Mounts = append(response.Mounts,
        &kubeletdevicepluginv1beta1.Mount{
            ContainerPath: fmt.Sprintf("%s/vgpu/libvgpu.so", hostHookPath),
            HostPath:      GetLibPath(),
            ReadOnly:      true,
        },
    )
    
    // 挂载 ld.so.preload（注入 vGPU 库）
    response.Mounts = append(response.Mounts, 
        &kubeletdevicepluginv1beta1.Mount{
            ContainerPath: "/etc/ld.so.preload",
            HostPath:      hostHookPath + "/vgpu/ld.so.preload",
            ReadOnly:      true,
        },
    )
}
```

**这段代码做了什么？**

1. **设置环境变量**：
   - `CUDA_DEVICE_MEMORY_LIMIT_0=4000m`：限制 GPU 0 的显存为 4GB
   - `CUDA_DEVICE_SM_LIMIT=50`：限制算力为 50%

2. **挂载 vGPU 库**：
   - `libvgpu.so`：HAMi 的核心库，拦截 CUDA API
   - `/etc/ld.so.preload`：让容器启动时自动加载 vGPU 库

3. **创建隔离环境**：
   - 为每个容器创建独立的缓存目录
   - 防止不同容器之间的资源冲突

#### 3. 与 Scheduler 的协作

**关键代码**（`server.go:620-635`）：

```go
// 从 Pod 注解读取 Scheduler 的分配决策
currentCtr, devreq, err := GetNextDeviceRequest(nvidia.NvidiaGPUDevice, *current)
klog.Infoln("deviceAllocateFromAnnotation=", devreq)

// 验证设备数量是否匹配
if len(devreq) != len(reqs.ContainerRequests[idx].DevicesIDs) {
    return &kubeletdevicepluginv1beta1.AllocateResponse{}, errors.New("device number not matched")
}

// 获取分配响应
response, err := plugin.getAllocateResponse(plugin.GetContainerDeviceStrArray(devreq))

// 从注解中删除已处理的设备请求
err = EraseNextDeviceTypeFromAnnotation(nvidia.NvidiaGPUDevice, *current)
```

**这段代码做了什么？**

1. **读取 Scheduler 决策**：从 Pod 注解 `hami.io/vgpu-devices-to-allocate` 读取分配的设备
2. **验证一致性**：确保 Scheduler 分配的设备数量与 kubelet 请求的一致
3. **清理注解**：处理完后删除注解，避免重复处理

**NVIDIA 官方插件没有这个逻辑**，因为它不需要与外部 Scheduler 协作。

#### 4. 动态 MIG 支持

**关键代码**（`server.go:300-340`）：

```go
// 检测设备是否支持 MIG
var deviceSupportMig bool
for _, name := range deviceNames {
    for _, migTemplate := range plugin.schedulerConfig.MigGeometriesList {
        if containsModel(name, migTemplate.Models) {
            deviceSupportMig = true
            break
        }
    }
}

if deviceSupportMig {
    // 导出当前 MIG 配置
    cmd := exec.Command("nvidia-mig-parted", "export")
    // ...
    
    if plugin.operatingMode == "mig" {
        // 处理 MIG 配置
        HamiInitMigConfig, err := plugin.processMigConfigs(plugin.migCurrent.MigConfigs, deviceNumbers)
        plugin.migCurrent.MigConfigs["current"] = HamiInitMigConfig
    }
}

// 应用 MIG 模板
if deviceSupportMig {
    plugin.ApplyMigTemplate()
}
```

**这段代码做了什么？**

1. **检测 MIG 支持**：根据 GPU 型号判断是否支持 MIG
2. **读取 MIG 配置**：调用 `nvidia-mig-parted` 工具
3. **动态应用配置**：根据调度需求动态切换 MIG 模式

**NVIDIA 官方插件**只支持静态 MIG 配置，不支持动态切换。

### 代码复用情况

HAMi Device Plugin 复用了 NVIDIA 官方插件的：

| 组件 | 是否复用 | 说明 |
|------|---------|------|
| **gRPC 服务框架** | ✅ 复用 | 基本的 Device Plugin 服务器逻辑 |
| **设备发现** | ✅ 复用 | 使用 NVML 枚举 GPU |
| **健康检查** | ✅ 复用 | 监控设备健康状态 |
| **CDI 支持** | ✅ 复用 | Container Device Interface |
| **Allocate 逻辑** | ❌ 重写 | 增加了与 Scheduler 协作、资源隔离 |
| **资源上报** | ⚠️ 修改 | 支持虚拟化（一个物理 GPU 上报多个虚拟 GPU） |

### 依赖关系

```go
// go.mod 中的关键依赖
require (
    github.com/NVIDIA/go-nvml v0.13.0-1           // NVML 库（设备发现）
    github.com/NVIDIA/k8s-device-plugin v0.18.2   // NVIDIA 官方插件（框架）
    github.com/NVIDIA/nvidia-container-toolkit v1.18.2  // 容器运行时集成
)
```

**HAMi 依赖 NVIDIA 官方库，但重写了核心逻辑**。

---

## 总结对比

### NVIDIA 官方 Device Plugin vs HAMi Device Plugin

| 特性 | NVIDIA 官方 | HAMi |
|------|------------|------|
| **资源粒度** | 整卡 | 显存 + 算力 |
| **资源隔离** | 无（Time-Slicing 无隔离） | 硬隔离（vGPU 库） |
| **调度器集成** | 无需外部调度器 | 与 HAMi Scheduler 协作 |
| **MIG 支持** | 静态配置 | 动态切换 |
| **超分配** | 不支持 | 支持（显存超分） |
| **适用场景** | 训练（独占 GPU） | 推理（共享 GPU） |

### 为什么不直接用 NVIDIA 官方插件？

**NVIDIA 官方插件的设计目标**：
- 为训练任务提供整卡资源
- 简单、稳定、官方支持

**HAMi 的设计目标**：
- 为推理服务提供细粒度共享
- 提高 GPU 利用率
- 降低成本

**两者的目标不同，所以需要不同的实现**。

### 技术实现对比

```
NVIDIA 官方插件：
┌─────────────┐
│   kubelet   │
└──────┬──────┘
       │ Allocate(gpu-0)
       ↓
┌─────────────────────┐
│ NVIDIA Device Plugin│
└──────┬──────────────┘
       │ 返回：NVIDIA_VISIBLE_DEVICES=GPU-uuid-0
       ↓
┌─────────────┐
│  Container  │  ← 可以使用整个 GPU
└─────────────┘


HAMi Device Plugin：
┌─────────────┐
│  Scheduler  │  ← 决定分配哪个 GPU 的哪部分资源
└──────┬──────┘
       │ 写入 Pod 注解：gpu-0, 4GB, 50%
       ↓
┌─────────────┐
│   kubelet   │
└──────┬──────┘
       │ Allocate(gpu-0)
       ↓
┌─────────────────────┐
│  HAMi Device Plugin │  ← 读取注解，设置资源限制
└──────┬──────────────┘
       │ 返回：
       │ - NVIDIA_VISIBLE_DEVICES=GPU-uuid-0
       │ - CUDA_DEVICE_MEMORY_LIMIT_0=4000m
       │ - CUDA_DEVICE_SM_LIMIT=50
       │ - 挂载 libvgpu.so
       ↓
┌─────────────┐
│  Container  │  ← vGPU 库拦截 CUDA API，限制资源使用
└─────────────┘
```

---

## 关键技术：vGPU 库（libvgpu.so）

这是 HAMi 实现资源隔离的核心技术：

### 工作原理

1. **LD_PRELOAD 机制**：
   ```bash
   # /etc/ld.so.preload 内容
   /usr/local/vgpu/libvgpu.so
   ```
   容器启动时，动态链接器会先加载 `libvgpu.so`

2. **API 拦截**：
   ```c
   // libvgpu.so 中的代码（伪代码）
   cudaError_t cudaMalloc(void **devPtr, size_t size) {
       // 检查是否超过限制
       if (current_usage + size > CUDA_DEVICE_MEMORY_LIMIT) {
           return cudaErrorMemoryAllocation;
       }
       
       // 调用真正的 cudaMalloc
       return real_cudaMalloc(devPtr, size);
   }
   ```

3. **资源统计**：
   - 记录每次内存分配
   - 统计 GPU 使用时间
   - 限制 SM 使用率

### 为什么 NVIDIA 官方插件不这样做？

1. **性能开销**：API 拦截有性能损耗（约 1-5%）
2. **复杂性**：需要维护 vGPU 库，适配不同 CUDA 版本
3. **目标不同**：官方插件主要服务训练场景，不需要细粒度共享

---

## 最终答案

### 问题 1：如何与 Volcano 集成？

**通过独立的 volcano-vgpu-device-plugin 项目**，而非硬编码：
- Volcano 内置 `deviceshare` 插件（读取 `volcano.sh/vgpu-*` 资源）
- volcano-vgpu-device-plugin 上报这些资源
- 两者通过标准的 Kubernetes API 和注解通信
- 复用 HAMi-core 的资源隔离能力

### 问题 2：为什么自己实现 Device Plugin？

**基于 NVIDIA 官方插件修改，增加细粒度共享能力**：
- 复用官方插件的框架和设备发现逻辑
- 重写 Allocate 方法，增加与 Scheduler 协作
- 注入 vGPU 库，实现硬资源隔离
- 支持动态 MIG 和资源超分配
- 满足推理服务的细粒度共享需求

**核心差异**：NVIDIA 官方插件服务训练场景（整卡独占），HAMi 服务推理场景（细粒度共享）。

