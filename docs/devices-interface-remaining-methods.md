# Devices 接口剩余方法的详细注释

由于文件大小限制，这里补充 Devices 接口中剩余方法的详细注释说明。

## LockNode - 获取节点分布式锁

```go
// LockNode 获取节点的分布式锁
// 在 Bind 阶段调用，防止并发调度导致设备重复分配
//
// 主要功能：
// 1. 获取锁：使用 Kubernetes Lease 对象实现分布式锁
// 2. 超时控制：锁有超时时间（默认 5 分钟），防止死锁
// 3. 并发控制：确保同一时刻只有一个 Pod 在该节点上分配设备
//
// 为什么需要节点锁？
// 问题场景：
//   - Pod A 和 Pod B 同时调度到 Node1
//   - Filter 阶段都看到 GPU-0 有 16GB 空闲
//   - 如果没有锁，两个 Pod 都会分配 GPU-0
//   - 实际 GPU-0 只有 16GB，导致资源超分
//
// 解决方案：
//   - Bind 阶段获取节点锁
//   - 先获取锁的 Pod 完成设备分配
//   - 后获取锁的 Pod 看到更新后的设备状态
//
// 锁的实现：
// - 使用 Kubernetes Lease 对象
// - Lease 名称：hami.io/mutex.lock-{nodeName}
// - 锁持有者：Pod 的 UID
// - 自动过期：超时后自动释放
//
// 实现示例（NVIDIA）：
//   func (dev *NvidiaGPUDevices) LockNode(n *corev1.Node, p *corev1.Pod) error {
//       // 检查 Pod 是否请求了设备
//       hasDevice := false
//       for _, ctr := range p.Spec.Containers {
//           if dev.GenerateResourceRequests(&ctr).Nums > 0 {
//               hasDevice = true
//               break
//           }
//       }
//       if !hasDevice {
//           return nil  // 不需要设备，跳过加锁
//       }
//
//       // 获取节点锁
//       return nodelock.LockNode(n.Name, "hami.io/mutex.lock", p)
//   }
//
// 调用时机：
// - Scheduler.Bind() 方法开始时
// - 在实际绑定 Pod 之前
//
// 参数：
// - n: 要锁定的节点
// - p: 请求锁的 Pod
//
// 返回值：
// - error: 获取锁失败时返回错误（会导致绑定失败）
LockNode(n *corev1.Node, p *corev1.Pod) error
```

## ReleaseNodeLock - 释放节点分布式锁

```go
// ReleaseNodeLock 释放节点的分布式锁
// 在 Bind 完成后调用，无论成功还是失败都要释放
//
// 主要功能：
// 1. 释放锁：删除或更新 Lease 对象
// 2. 清理状态：确保锁不会被永久持有
// 3. 错误处理：即使释放失败也要记录日志
//
// 为什么必须释放锁？
// - 避免死锁：如果不释放，其他 Pod 无法调度到该节点
// - 资源回收：Lease 对象会占用 etcd 空间
// - 超时兜底：即使忘记释放，锁也会在超时后自动释放
//
// 释放时机：
// - 绑定成功后
// - 绑定失败后（使用 goto 确保一定执行）
// - 任何错误情况下
//
// 实现示例（NVIDIA）：
//   func (dev *NvidiaGPUDevices) ReleaseNodeLock(n *corev1.Node, p *corev1.Pod) error {
//       // 检查 Pod 是否请求了设备
//       hasDevice := false
//       for _, ctr := range p.Spec.Containers {
//           if dev.GenerateResourceRequests(&ctr).Nums > 0 {
//               hasDevice = true
//               break
//           }
//       }
//       if !hasDevice {
//           return nil  // 没有加锁，跳过释放
//       }
//
//       // 释放节点锁
//       return nodelock.ReleaseNodeLock(n.Name, "hami.io/mutex.lock", p, false)
//   }
//
// 参数：
// - n: 要释放锁的节点
// - p: 持有锁的 Pod
//
// 返回值：
// - error: 释放锁失败时返回错误（通常只记录日志，不影响流程）
ReleaseNodeLock(n *corev1.Node, p *corev1.Pod) error
```

## GenerateResourceRequests - 提取设备需求

```go
// GenerateResourceRequests 从容器的资源请求中提取设备需求
// 这是 Scheduler 解析 Pod 资源请求的方法
//
// 主要功能：
// 1. 读取资源：从 Limits 或 Requests 中读取设备资源
// 2. 解析数量：提取设备数量、显存、算力等需求
// 3. 应用默认值：为未指定的资源设置默认值
// 4. 格式转换：将 Kubernetes 资源格式转换为内部格式
//
// 资源读取优先级：
// 1. 优先读取 Limits
// 2. 如果 Limits 不存在，读取 Requests
// 3. 如果都不存在，使用配置的默认值
//
// 为什么需要这个方法？
// - 统一格式：将 Kubernetes 的资源格式转换为调度器内部格式
// - 默认值处理：简化用户配置，自动补全未指定的资源
// - 多维度资源：支持设备数量、显存、算力等多个维度
//
// 实现示例（NVIDIA）：
//   func (dev *NvidiaGPUDevices) GenerateResourceRequests(ctr *corev1.Container) ContainerDeviceRequest {
//       // 读取 GPU 数量
//       gpuCount, ok := ctr.Resources.Limits["nvidia.com/gpu"]
//       if !ok {
//           return ContainerDeviceRequest{}  // 未请求 GPU
//       }
//
//       // 读取显存需求
//       memReq := int32(0)
//       if mem, ok := ctr.Resources.Limits["nvidia.com/gpumem"]; ok {
//           memReq = int32(mem.Value())
//       } else {
//           memReq = dev.config.DefaultMemory  // 使用默认值
//       }
//
//       // 读取算力需求
//       coreReq := int32(0)
//       if core, ok := ctr.Resources.Limits["nvidia.com/gpucores"]; ok {
//           coreReq = int32(core.Value())
//       } else {
//           coreReq = dev.config.DefaultCores  // 使用默认值
//       }
//
//       return ContainerDeviceRequest{
//           Nums:     int32(gpuCount.Value()),
//           Type:     "NVIDIA",
//           Memreq:   memReq,
//           Coresreq: coreReq,
//       }
//   }
//
// 调用时机：
// - Scheduler.Filter() 方法中
// - 为每个容器生成设备请求
//
// 参数：
// - ctr: 容器对象
//
// 返回值：
// - ContainerDeviceRequest: 容器的设备需求
//   - Nums: 设备数量
//   - Type: 设备类型
//   - Memreq: 显存需求（MB）
//   - Coresreq: 算力需求（百分比）
GenerateResourceRequests(ctr *corev1.Container) ContainerDeviceRequest
```

## PatchAnnotations - 写入设备分配结果

```go
// PatchAnnotations 将设备分配结果写入 Pod 注解
// 这是 Scheduler 记录分配结果的方法
//
// 主要功能：
// 1. 编码设备信息：将设备分配结果编码为字符串
// 2. 写入注解：添加到 Pod 的 Annotations 中
// 3. 多设备支持：处理 Pod 使用多种设备类型的情况
//
// 注解格式：
// - Key: 从 SupportDevices map 中获取
//   例如：hami.io/vgpu-devices-allocated
// - Value: 编码后的设备信息
//   例如：GPU-xxx,NVIDIA,4096,50;GPU-yyy,NVIDIA,4096,50
//
// 为什么需要这个方法？
// - 数据传递：Scheduler 通过注解将分配结果传递给 Device Plugin
// - 持久化：注解存储在 etcd 中，支持故障恢复
// - 可追溯：可以查看 Pod 使用了哪些设备
//
// 实现示例（NVIDIA）：
//   func (dev *NvidiaGPUDevices) PatchAnnotations(pod *corev1.Pod, annoinput *map[string]string, pd PodDevices) map[string]string {
//       // 获取 NVIDIA 设备的分配结果
//       devlist, ok := pd["NVIDIA"]
//       if !ok || len(devlist) == 0 {
//           return *annoinput  // 没有分配 NVIDIA 设备
//       }
//
//       // 编码设备信息
//       deviceStr := device.EncodePodSingleDevice(devlist)
//
//       // 写入注解
//       (*annoinput)["hami.io/vgpu-devices-to-allocate"] = deviceStr
//       (*annoinput)["hami.io/vgpu-devices-allocated"] = deviceStr
//
//       return *annoinput
//   }
//
// 调用时机：
// - Scheduler.Filter() 方法中
// - 选定节点和设备后
//
// 参数：
// - pod: Pod 对象
// - annoinput: 注解 map（会被修改）
// - pd: 设备分配结果
//
// 返回值：
// - map[string]string: 更新后的注解 map
PatchAnnotations(pod *corev1.Pod, annoinput *map[string]string, pd PodDevices) map[string]string
```

## ScoreNode - 计算节点额外分数

```go
// ScoreNode 计算节点的额外分数
// 这是设备特定的打分逻辑，在基础分数上叠加
//
// 主要功能：
// 1. 拓扑感知：考虑设备间的通信性能（NVLink、PCIe 拓扑）
// 2. NUMA 亲和性：优先选择同 NUMA 节点的设备
// 3. 设备特性：考虑设备型号、代际等因素
// 4. 自定义策略：实现设备特定的调度偏好
//
// 打分场景：
// - 拓扑调度：多卡训练任务需要高速互联的 GPU
// - 负载均衡：避免某些设备过载
// - 性能优化：选择性能更好的设备组合
//
// 为什么需要这个方法？
// - 设备特性：不同设备有不同的性能特征
// - 拓扑优化：GPU 间的通信性能差异很大
// - 灵活性：支持设备特定的调度策略
//
// 实现示例（NVIDIA - 拓扑感知）：
//   func (dev *NvidiaGPUDevices) ScoreNode(node *corev1.Node, podDevices PodSingleDevice, previous []*DeviceUsage, policy string) float32 {
//       // 如果只请求一个 GPU，不需要考虑拓扑
//       if len(podDevices) <= 1 {
//           return 0
//       }
//
//       // 计算设备间的拓扑分数
//       totalScore := 0
//       for i := 0; i < len(podDevices)-1; i++ {
//           for j := i+1; j < len(podDevices); j++ {
//               // 从拓扑分数表中查询
//               score := getTopologyScore(podDevices[i].UUID, podDevices[j].UUID)
//               totalScore += score
//           }
//       }
//
//       // 归一化分数
//       return float32(totalScore) / 100.0
//   }
//
// 调用时机：
// - Scheduler.calcScore() 方法中
// - 在 ComputeDefaultScore 之后
//
// 参数：
// - node: 节点对象
// - podDevices: 分配给 Pod 的设备列表
// - previous: 分配前的设备状态快照
// - policy: 调度策略（binpack/spread）
//
// 返回值：
// - float32: 额外的分数（会加到节点的基础分数上）
ScoreNode(node *corev1.Node, podDevices PodSingleDevice, previous []*DeviceUsage, policy string) float32
```

## AddResourceUsage - 更新设备使用量

```go
// AddResourceUsage 更新设备的资源使用量
// 在设备匹配成功后调用，记录资源分配
//
// 主要功能：
// 1. 更新使用量：累加设备的已用显存、算力
// 2. 更新计数：增加设备的使用次数（时间切片）
// 3. MIG 处理：标记 MIG 实例为已使用
// 4. UUID 更新：为 MIG 设备添加实例标识
//
// 为什么需要这个方法？
// - 状态更新：Fit 方法只判断是否满足，不修改状态
// - 多容器支持：同一个 Pod 的多个容器依次分配设备
// - 准确计算：确保后续容器看到更新后的设备状态
//
// MIG 特殊处理：
// - MIG 允许将一个 GPU 分割成多个实例
// - 需要选择合适的 MIG 模板（根据显存需求）
// - 标记对应的 MIG 实例为已使用
// - 更新 UUID 格式：GPU-xxx[template-instance]
//
// 实现示例（NVIDIA）：
//   func (dev *NvidiaGPUDevices) AddResourceUsage(pod *corev1.Pod, n *DeviceUsage, ctr *ContainerDevice) error {
//       // 增加使用计数
//       n.Used++
//
//       // 如果是 MIG 模式
//       if n.Mode == "mig" {
//           // 选择合适的 MIG 模板
//           for tidx, templates := range n.MigTemplate {
//               for idx, template := range templates {
//                   if template.Memory >= ctr.Usedmem {
//                       // 找到合适的模板，标记为已使用
//                       n.MigUsage.UsageList[idx].InUse = true
//                       // 更新 UUID
//                       ctr.UUID = fmt.Sprintf("%s[%d-%d]", ctr.UUID, tidx, idx)
//                       break
//                   }
//               }
//           }
//       }
//
//       // 累加资源使用量
//       n.Usedmem += ctr.Usedmem
//       n.Usedcores += ctr.Usedcores
//
//       return nil
//   }
//
// 调用时机：
// - fitInDevices() 方法中
// - Fit 方法返回 true 后
//
// 参数：
// - pod: Pod 对象
// - n: 设备使用情况（会被修改）
// - ctr: 容器设备分配结果（会被修改，如 UUID）
//
// 返回值：
// - error: 更新失败时返回错误
AddResourceUsage(pod *corev1.Pod, n *DeviceUsage, ctr *ContainerDevice) error
```

## Fit - 设备匹配核心方法

```go
// Fit 判断节点是否能满足容器的设备需求
// 这是调度决策的核心方法，决定设备分配
//
// 主要功能：
// 1. 设备过滤：根据类型、健康状态、UUID 等过滤设备
// 2. 资源检查：检查显存、算力是否足够
// 3. 配额验证：检查命名空间配额是否足够
// 4. 设备选择：选择最合适的设备组合
// 5. 拓扑优化：考虑设备间的通信性能（可选）
//
// 过滤条件（按顺序）：
// 1. 健康状态：设备必须健康
// 2. 设备类型：匹配用户指定的 GPU 型号
// 3. UUID 过滤：匹配用户指定的 GPU UUID
// 4. NUMA 亲和性：如果启用，必须在同一 NUMA 节点
// 5. 时间切片：设备的并发数未超限
// 6. 显存充足：剩余显存 >= 请求显存
// 7. 算力充足：剩余算力 >= 请求算力
// 8. 独占冲突：独占设备不能被共享
// 9. 配额限制：命名空间配额未超限
// 10. 自定义规则：设备特定的过滤规则（如 MIG）
//
// 为什么需要这么多过滤条件？
// - 资源保证：确保分配的设备真正满足需求
// - 性能优化：选择最合适的设备组合
// - 隔离性：避免资源冲突和干扰
// - 灵活性：支持用户的各种调度需求
//
// 实现示例（NVIDIA - 简化版）：
//   func (dev *NvidiaGPUDevices) Fit(devices []*DeviceUsage, request ContainerDeviceRequest, pod *corev1.Pod, nodeInfo *NodeInfo, allocated *PodDevices) (bool, map[string]ContainerDevices, string) {
//       tmpDevs := make(map[string]ContainerDevices)
//       needCount := request.Nums
//
//       // 遍历设备列表（已按策略排序）
//       for _, dev := range devices {
//           // 1. 检查健康状态
//           if !dev.Health {
//               continue
//           }
//
//           // 2. 检查设备类型
//           if !checkGPUType(pod.Annotations, dev.Type) {
//               continue
//           }
//
//           // 3. 检查显存
//           if dev.Totalmem - dev.Usedmem < request.Memreq {
//               continue
//           }
//
//           // 4. 检查算力
//           if dev.Totalcore - dev.Usedcores < request.Coresreq {
//               continue
//           }
//
//           // 5. 检查配额
//           if !fitQuota(tmpDevs, allocated, pod.Namespace, request.Memreq, request.Coresreq) {
//               continue
//           }
//
//           // 找到合适的设备
//           tmpDevs[request.Type] = append(tmpDevs[request.Type], ContainerDevice{
//               UUID:      dev.ID,
//               Type:      request.Type,
//               Usedmem:   request.Memreq,
//               Usedcores: request.Coresreq,
//           })
//
//           needCount--
//           if needCount == 0 {
//               return true, tmpDevs, ""  // 成功分配
//           }
//       }
//
//       // 设备不足
//       return false, tmpDevs, "insufficient devices"
//   }
//
// 调用时机：
// - calcScore() 方法中
// - 为每个容器调用一次
//
// 参数：
// - devices: 节点上的设备列表（已排序）
// - request: 容器的设备需求
// - pod: Pod 对象（用于读取注解）
// - nodeInfo: 节点信息（包含拓扑等）
// - allocated: 已分配给该 Pod 的设备（用于多容器场景）
//
// 返回值：
// - bool: 是否找到合适的设备
// - map[string]ContainerDevices: 分配的设备列表（按设备类型分组）
// - string: 失败原因（用于日志和事件）
Fit(devices []*DeviceUsage, request ContainerDeviceRequest, pod *corev1.Pod, nodeInfo *NodeInfo, allocated *PodDevices) (bool, map[string]ContainerDevices, string)
```

## 总结

Devices 接口的 13 个方法涵盖了设备管理的完整生命周期：

1. **基础信息**：CommonWord, GetResourceNames
2. **准入控制**：MutateAdmission
3. **健康管理**：CheckHealth, NodeCleanUp
4. **设备发现**：GetNodeDevices
5. **调度决策**：GenerateResourceRequests, Fit, ScoreNode
6. **资源管理**：AddResourceUsage, PatchAnnotations
7. **并发控制**：LockNode, ReleaseNodeLock

这种设计使得 HAMi 可以轻松支持新的设备类型，只需实现这个接口即可。

