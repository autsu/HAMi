/*
Copyright 2024 The HAMi Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package device

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ccoveille/go-safecast"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/Project-HAMi/HAMi/pkg/util"
)

// Devices 是所有设备类型的统一抽象接口
// HAMi 通过这个接口支持多种异构设备（NVIDIA GPU、AMD GPU、华为昇腾 NPU 等）
//
// 设计理念：
// - 统一接口：不同厂商的设备实现相同的接口，调度器无需关心具体设备类型
// - 可扩展性：添加新设备类型只需实现此接口，无需修改调度器核心逻辑
// - 职责分离：设备特定的逻辑（如 MIG、拓扑感知）由各自实现处理
//
// 接口方法分类：
// 1. 基础信息：CommonWord, GetResourceNames
// 2. 生命周期：MutateAdmission, CheckHealth, NodeCleanUp
// 3. 设备发现：GetNodeDevices
// 4. 调度决策：GenerateResourceRequests, Fit, ScoreNode
// 5. 资源管理：AddResourceUsage, PatchAnnotations
// 6. 并发控制：LockNode, ReleaseNodeLock
//
// 实现示例：
// - pkg/device/nvidia/device.go: NvidiaGPUDevices
// - pkg/device/amd/device.go: AMDGPUDevices
// - pkg/device/ascend/device.go: AscendDevices
type Devices interface {
	// CommonWord 返回设备类型的标识名称
	// 用途：
	// - 作为 map 的 key 来区分不同设备类型
	// - 日志输出和错误信息中标识设备类型
	// - 与配置文件中的设备类型名称对应
	//
	// 返回值示例：
	// - "NVIDIA": NVIDIA GPU
	// - "AMD": AMD GPU
	// - "Ascend910A": 华为昇腾 910A
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) CommonWord() string {
	//       return "NVIDIA"
	//   }
	CommonWord() string

	// MutateAdmission 在 Pod 创建时修改容器的资源请求
	// 这是 Webhook 准入控制阶段调用的方法
	//
	// 主要功能：
	// 1. 资源规范化：将用户的资源请求转换为标准格式
	//    例如：只指定显存时，自动添加 GPU 数量请求
	// 2. 默认值填充：为未指定的资源设置默认值
	//    例如：请求 GPU 但未指定算力时，设置默认算力
	// 3. 环境变量注入：添加设备特定的环境变量
	//    例如：NVIDIA_VISIBLE_DEVICES, CUDA_DEVICE_ORDER
	// 4. 运行时类设置：设置 Pod 的 runtimeClassName
	//    例如：nvidia-container-runtime
	//
	// 为什么需要这个方法？
	// - 用户体验：简化用户的资源请求，自动补全必要信息
	// - 兼容性：处理不同版本 API 的资源请求格式
	// - 设备特性：注入设备特定的配置（如 MIG 模式、MPS 模式）
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) MutateAdmission(ctr *corev1.Container, pod *corev1.Pod) (bool, error) {
	//       // 检查是否请求了 GPU
	//       _, ok := ctr.Resources.Limits["nvidia.com/gpu"]
	//       if !ok {
	//           return false, nil  // 未请求 GPU，跳过
	//       }
	//
	//       // 设置默认显存（如果未指定）
	//       if _, ok := ctr.Resources.Limits["nvidia.com/gpumem"]; !ok {
	//           ctr.Resources.Limits["nvidia.com/gpumem"] = resource.NewQuantity(4096, resource.BinarySI)
	//       }
	//
	//       // 设置运行时类
	//       if pod.Spec.RuntimeClassName == nil {
	//           runtimeClass := "nvidia"
	//           pod.Spec.RuntimeClassName = &runtimeClass
	//       }
	//
	//       return true, nil  // 返回 true 表示容器请求了此类型设备
	//   }
	//
	// 参数：
	// - ctr: 容器对象，可以修改其资源请求和环境变量
	// - pod: Pod 对象，可以修改其 Spec（如 runtimeClassName）
	//
	// 返回值：
	// - bool: 容器是否请求了此类型的设备
	// - error: 验证失败时返回错误（会拒绝 Pod 创建）
	MutateAdmission(ctr *corev1.Container, pod *corev1.Pod) (bool, error)

	// CheckHealth 检查节点上设备的健康状态
	// 这是 Scheduler 定期调用的方法（通常每 15 秒一次）
	//
	// 主要功能：
	// 1. 设备可用性检查：设备是否在线、驱动是否正常
	// 2. 数量变化检测：设备数量是否发生变化（新增、移除）
	// 3. 状态同步：判断是否需要重新读取设备信息
	//
	// 检查方式：
	// - 读取节点的 Status.Allocatable 中的设备数量
	// - 与上次记录的数量对比
	// - 检查设备注解是否存在和有效
	//
	// 为什么需要这个方法？
	// - 故障检测：及时发现设备故障或 Device Plugin 崩溃
	// - 动态更新：支持热插拔设备（虽然 GPU 通常不支持）
	// - 状态同步：确保 Scheduler 的设备信息与实际状态一致
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) CheckHealth(devType string, n *corev1.Node) (bool, bool) {
	//       // 获取当前设备数量
	//       current := n.Status.Allocatable["nvidia.com/gpu"].Value()
	//
	//       // 获取上次记录的数量
	//       reported := dev.ReportedGPUNum[n.Name]
	//
	//       if current == 0 && reported > 0 {
	//           // 设备消失了，标记为不健康
	//           return false, false
	//       }
	//
	//       if current != reported {
	//           // 设备数量变化，需要更新
	//           dev.ReportedGPUNum[n.Name] = current
	//           return true, true
	//       }
	//
	//       // 一切正常
	//       return true, false
	//   }
	//
	// 参数：
	// - devType: 设备类型标识（与 CommonWord() 返回值相同）
	// - n: Kubernetes 节点对象
	//
	// 返回值：
	// - 第一个 bool: 设备是否健康（false 会触发节点清理）
	// - 第二个 bool: 是否需要更新设备信息（true 会重新调用 GetNodeDevices）
	CheckHealth(devType string, n *corev1.Node) (bool, bool)

	// NodeCleanUp 清理节点的设备相关注解
	// 当设备不健康或节点被删除时调用
	//
	// 主要功能：
	// 1. 删除设备注册注解：移除设备信息注解
	// 2. 删除握手注解：标记设备已下线
	// 3. 触发重新注册：Device Plugin 会检测到注解变化并重新注册
	//
	// 为什么需要这个方法？
	// - 故障恢复：清理过期的设备信息，等待 Device Plugin 重新上报
	// - 状态一致性：确保 Scheduler 不会使用已失效的设备信息
	// - 避免误调度：防止将 Pod 调度到已故障的设备上
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) NodeCleanUp(nn string) error {
	//       // 标记握手注解为 "Deleted"
	//       return util.MarkAnnotationsToDelete("hami.io/node-handshake", nn)
	//   }
	//
	// 调用时机：
	// - CheckHealth 返回 (false, false) 时
	// - 节点被删除时（Informer 的 DeleteFunc）
	//
	// 参数：
	// - nn: 节点名称
	//
	// 返回值：
	// - error: 清理失败时返回错误
	NodeCleanUp(nn string) error

	// GetResourceNames 返回设备的 Kubernetes 资源名称
	// 用于从容器的资源请求中提取设备需求
	//
	// 资源名称类型：
	// 1. ResourceCountName: 设备数量（必需）
	//    例如：nvidia.com/gpu, amd.com/gpu
	// 2. ResourceMemoryName: 设备显存（可选）
	//    例如：nvidia.com/gpumem, amd.com/gpumem
	// 3. ResourceCoreName: 设备算力（可选）
	//    例如：nvidia.com/gpucores, amd.com/gpucores
	//
	// 为什么需要这个方法？
	// - 资源解析：Scheduler 需要知道如何从容器请求中提取设备需求
	// - 多设备支持：不同设备使用不同的资源名称
	// - 配置灵活性：资源名称可以通过配置文件自定义
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) GetResourceNames() ResourceNames {
	//       return ResourceNames{
	//           ResourceCountName:  "nvidia.com/gpu",
	//           ResourceMemoryName: "nvidia.com/gpumem",
	//           ResourceCoreName:   "nvidia.com/gpucores",
	//       }
	//   }
	//
	// 返回值：
	// - ResourceNames: 包含三种资源名称的结构体
	GetResourceNames() ResourceNames

	// GetNodeDevices 从节点注解中读取设备信息
	// 这是 Scheduler 获取节点设备列表的方法
	//
	// 主要功能：
	// 1. 读取注解：从节点的 Annotations 中读取设备信息
	// 2. 解析数据：将注解字符串解析为 DeviceInfo 结构体
	// 3. 补充信息：添加设备特定的额外信息（如 MIG 模板、拓扑分数）
	// 4. 验证数据：检查设备信息的完整性和有效性
	//
	// 设备信息来源：
	// - Device Plugin 将设备信息写入节点注解
	// - 注解格式：通常是 JSON 或自定义编码格式
	// - 包含：UUID、显存、算力、NUMA、健康状态等
	//
	// 为什么需要这个方法？
	// - 数据源：Scheduler 通过节点注解获取设备信息
	// - 解耦：Device Plugin 和 Scheduler 通过注解通信
	// - 持久化：注解存储在 etcd 中，支持 Scheduler 重启恢复
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) GetNodeDevices(n corev1.Node) ([]*DeviceInfo, error) {
	//       // 读取设备注册注解
	//       devEncoded, ok := n.Annotations["hami.io/node-nvidia-register"]
	//       if !ok {
	//           return nil, errors.New("device annotation not found")
	//       }
	//
	//       // 解析设备信息
	//       devices, err := device.UnMarshalNodeDevices(devEncoded)
	//       if err != nil {
	//           return nil, err
	//       }
	//
	//       // 补充 MIG 模板信息
	//       for _, dev := range devices {
	//           if dev.Mode == "mig" {
	//               dev.MIGTemplate = dev.config.MigGeometriesList
	//           }
	//       }
	//
	//       return devices, nil
	//   }
	//
	// 参数：
	// - n: Kubernetes 节点对象
	//
	// 返回值：
	// - []*DeviceInfo: 节点上的设备列表
	// - error: 读取或解析失败时返回错误
	GetNodeDevices(n corev1.Node) ([]*DeviceInfo, error)

	// GenerateResourceRequests 从容器的资源请求中提取设备需求
	// 这是调度流程的第一步：解析 Pod 的资源请求
	//
	// 主要功能：
	// 1. 读取容器的 Resources.Limits 中的设备资源
	// 2. 提取设备数量、显存、算力等需求
	// 3. 处理百分比显存请求（如 50% 显存）
	// 4. 返回标准化的设备请求结构
	//
	// 为什么需要这个方法？
	// - 资源解析：将 Kubernetes 资源请求转换为设备分配参数
	// - 灵活性：支持多种资源请求方式（绝对值、百分比）
	// - 验证：检查资源请求的合法性
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) GenerateResourceRequests(ctr *corev1.Container) ContainerDeviceRequest {
	//       request := ContainerDeviceRequest{Type: "NVIDIA"}
	//       // 提取 GPU 数量
	//       if val, ok := ctr.Resources.Limits["nvidia.com/gpu"]; ok {
	//           request.Nums = int32(val.Value())
	//       }
	//       // 提取显存需求（MB）
	//       if val, ok := ctr.Resources.Limits["nvidia.com/gpumem"]; ok {
	//           request.Memreq = int32(val.Value())
	//       }
	//       // 提取显存百分比需求（0-100）
	//       if val, ok := ctr.Resources.Limits["nvidia.com/gpumem-percentage"]; ok {
	//           request.MemPercentagereq = int32(val.Value())
	//       }
	//       // 提取算力需求（0-100）
	//       if val, ok := ctr.Resources.Limits["nvidia.com/gpucores"]; ok {
	//           request.Coresreq = int32(val.Value())
	//       }
	//       return request
	//   }
	//
	// 参数：
	// - ctr: 容器对象，包含资源请求信息
	//
	// 返回值：
	// - ContainerDeviceRequest: 标准化的设备请求，包含 Nums（数量）、Memreq（显存）、Coresreq（算力）等
	GenerateResourceRequests(ctr *corev1.Container) ContainerDeviceRequest

	// Fit 判断节点是否能满足 Pod 的设备需求，并分配具体设备
	// 这是调度流程的核心方法：Filter 阶段的设备匹配和分配
	//
	// 主要功能：
	// 1. 遍历节点上的可用设备
	// 2. 根据策略（binpack/spread）选择最优设备
	// 3. 检查设备资源是否充足（显存、算力）
	// 4. 处理特殊模式（MIG、MPS、NUMA 绑定）
	// 5. 返回分配结果
	//
	// 调度策略：
	// - binpack: 优先填满已使用的设备（提高利用率）
	// - spread: 优先使用空闲设备（降低干扰）
	//
	// 为什么需要这个方法？
	// - 资源匹配：判断节点是否有足够的设备资源
	// - 设备分配：为 Pod 选择具体的设备
	// - 策略实现：根据不同策略优化资源分配
	// - 约束处理：处理 NUMA、UUID 等约束条件
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) Fit(...) (bool, map[string]ContainerDevices, string) {
	//       // 1. 根据策略排序设备
	//       if policy == "binpack" {
	//           sort.Slice(devices, func(i, j int) bool {
	//               return devices[i].Used > devices[j].Used  // 优先使用已占用的设备
	//           })
	//       }
	//       // 2. 尝试分配设备
	//       allocated := make(ContainerDevices, 0)
	//       for _, device := range devices {
	//           // 检查显存和算力是否充足
	//           if device.Totalmem-device.Usedmem < request.Memreq { continue }
	//           if device.Totalcore-device.Usedcores < request.Coresreq { continue }
	//           // 分配成功
	//           allocated = append(allocated, ContainerDevice{UUID: device.ID, ...})
	//           if len(allocated) == int(request.Nums) { break }
	//       }
	//       return len(allocated) == int(request.Nums), map[string]ContainerDevices{"NVIDIA": allocated}, ""
	//   }
	//
	// 参数：
	// - devices: 节点上的设备列表（包含使用情况）
	// - request: 容器的设备需求
	// - pod: Pod 对象（用于读取注解约束）
	// - nodeInfo: 节点信息（包含拓扑信息）
	// - allocated: 已分配给该 Pod 其他容器的设备（避免重复分配）
	//
	// 返回值：
	// - bool: 是否能满足需求
	// - map[string]ContainerDevices: 分配的设备列表（key 是设备类型）
	// - string: 失败原因（用于日志和调试）
	Fit(devices []*DeviceUsage, request ContainerDeviceRequest, pod *corev1.Pod, nodeInfo *NodeInfo, allocated *PodDevices) (bool, map[string]ContainerDevices, string)

	// ScoreNode 为节点打分，用于 Score 阶段选择最优节点
	// 这是调度流程的第二步：在多个可用节点中选择最优的
	//
	// 主要功能：
	// 1. 根据设备使用情况计算分数
	// 2. 考虑设备拓扑关系（NVLink、PCIe）
	// 3. 考虑 NUMA 亲和性
	// 4. 根据策略调整分数（binpack 倾向高分，spread 倾向低分）
	//
	// 打分策略：
	// - binpack: 已使用设备越多，分数越高（鼓励集中使用）
	// - spread: 已使用设备越少，分数越高（鼓励分散使用）
	// - 拓扑感知: NVLink 连接的设备分数更高
	//
	// 为什么需要这个方法？
	// - 优化调度：在多个可用节点中选择最优的
	// - 性能优化：考虑设备间通信性能（NVLink）
	// - 负载均衡：根据策略平衡集群负载
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) ScoreNode(...) float32 {
	//       score := float32(0)
	//       // 1. 基础分数：根据设备使用率
	//       for _, device := range previous {
	//           utilization := float32(device.Used) / float32(device.Count)
	//           if policy == "binpack" {
	//               score += utilization * 100  // 使用率越高，分数越高
	//           } else {
	//               score += (1 - utilization) * 100  // 使用率越低，分数越高
	//           }
	//       }
	//       // 2. 拓扑分数：NVLink 连接加分
	//       // 3. NUMA 亲和性分数
	//       return score
	//   }
	//
	// 参数：
	// - node: 节点对象
	// - podDevices: 为该 Pod 分配的设备列表
	// - previous: 节点上所有设备的使用情况
	// - policy: 调度策略（"binpack" 或 "spread"）
	//
	// 返回值：
	// - float32: 节点分数（0-100，分数越高越优）
	ScoreNode(node *corev1.Node, podDevices PodSingleDevice, previous []*DeviceUsage, policy string) float32

	// AddResourceUsage 更新设备的资源使用情况
	// 在 Bind 阶段调用，记录设备被 Pod 使用
	//
	// 主要功能：
	// 1. 增加设备的已使用显存
	// 2. 增加设备的已使用算力
	// 3. 记录 Pod 信息到设备使用列表
	// 4. 更新设备的使用计数
	//
	// 为什么需要这个方法？
	// - 状态更新：记录设备被占用的资源
	// - 调度依据：后续调度需要知道设备的剩余资源
	// - 审计追踪：记录哪些 Pod 使用了哪些设备
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) AddResourceUsage(pod *corev1.Pod, deviceUsage *DeviceUsage, ctr *ContainerDevice) error {
	//       // 1. 更新显存使用
	//       deviceUsage.Usedmem += ctr.Usedmem
	//       if deviceUsage.Usedmem > deviceUsage.Totalmem {
	//           return fmt.Errorf("memory overflow")
	//       }
	//       // 2. 更新算力使用
	//       deviceUsage.Usedcores += ctr.Usedcores
	//       // 3. 记录 Pod 信息
	//       deviceUsage.PodInfos = append(deviceUsage.PodInfos, &PodInfo{...})
	//       // 4. 更新使用计数
	//       deviceUsage.Used++
	//       return nil
	//   }
	//
	// 参数：
	// - pod: Pod 对象
	// - n: 设备使用情况对象（会被修改）
	// - ctr: 容器分配的设备信息
	//
	// 返回值：
	// - error: 更新失败时返回错误（如资源溢出）
	AddResourceUsage(pod *corev1.Pod, n *DeviceUsage, ctr *ContainerDevice) error

	// PatchAnnotations 生成需要写入 Pod 的设备分配注解
	// 在 Bind 阶段调用，将分配结果写入 Pod 注解
	//
	// 主要功能：
	// 1. 将设备分配结果编码为字符串
	// 2. 生成"待分配设备"注解（用于 Device Plugin）
	// 3. 生成"已分配设备"注解（用于审计和恢复）
	// 4. 合并其他设备类型的注解
	//
	// 注解格式示例：
	// - "hami.io/vgpu-devices-to-allocate": "GPU-uuid-1,NVIDIA,4096,50:GPU-uuid-2,NVIDIA,4096,50"
	// - "hami.io/vgpu-devices-allocated": "GPU-uuid-1,NVIDIA,4096,50:GPU-uuid-2,NVIDIA,4096,50"
	//
	// 为什么需要这个方法？
	// - 通信机制：Scheduler 通过注解告诉 Device Plugin 分配哪些设备
	// - 持久化：分配结果存储在 etcd 中，支持故障恢复
	// - 审计：记录设备分配历史
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) PatchAnnotations(pod *corev1.Pod, annoinput *map[string]string, pd PodDevices) map[string]string {
	//       annotations := make(map[string]string)
	//       // 1. 编码设备分配结果
	//       deviceStr := EncodeContainerDevices(pd["NVIDIA"])
	//       // 2. 生成"待分配"注解（Device Plugin 读取）
	//       annotations["hami.io/vgpu-devices-to-allocate"] = deviceStr
	//       // 3. 生成"已分配"注解（审计用）
	//       annotations["hami.io/vgpu-devices-allocated"] = deviceStr
	//       return annotations
	//   }
	//
	// 参数：
	// - pod: Pod 对象
	// - annoinput: 输入的注解（可能包含其他设备类型的注解）
	// - pd: 设备分配结果
	//
	// 返回值：
	// - map[string]string: 需要写入 Pod 的注解
	PatchAnnotations(pod *corev1.Pod, annoinput *map[string]string, pd PodDevices) map[string]string

	// LockNode 在调度前锁定节点，防止并发调度冲突
	// 使用 Kubernetes Lease 机制实现分布式锁
	//
	// 主要功能：
	// 1. 尝试获取节点的 Lease 锁
	// 2. 设置锁的持有者为当前 Pod
	// 3. 设置锁的超时时间（防止死锁）
	// 4. 如果锁已被占用，返回错误
	//
	// 为什么需要这个方法？
	// - 并发控制：多个 Scheduler 实例可能同时调度到同一节点
	// - 资源一致性：防止超额分配设备资源
	// - 分布式协调：使用 Kubernetes 原生机制实现分布式锁
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) LockNode(n *corev1.Node, p *corev1.Pod) error {
	//       leaseName := fmt.Sprintf("hami-node-%s", n.Name)
	//       err := nodelock.LockNode(leaseName, string(p.UID), 30*time.Second)
	//       if err != nil {
	//           return fmt.Errorf("failed to lock node %s: %v", n.Name, err)
	//       }
	//       return nil
	//   }
	//
	// 调用时机：
	// - Filter 阶段之前：确保设备资源计算的准确性
	// - Bind 阶段之前：确保设备分配的原子性
	//
	// 参数：
	// - n: 节点对象
	// - p: Pod 对象（用于标识锁的持有者）
	//
	// 返回值：
	// - error: 获取锁失败时返回错误
	LockNode(n *corev1.Node, p *corev1.Pod) error

	// ReleaseNodeLock 释放节点锁
	// 在调度完成或失败后调用
	//
	// 主要功能：
	// 1. 删除节点的 Lease 锁
	// 2. 验证锁的持有者是当前 Pod
	// 3. 处理锁已过期的情况
	//
	// 为什么需要这个方法？
	// - 资源释放：及时释放锁，让其他调度可以进行
	// - 避免死锁：确保锁不会永久占用
	// - 错误恢复：处理调度失败的情况
	//
	// 实现示例（NVIDIA）：
	//   func (dev *NvidiaGPUDevices) ReleaseNodeLock(n *corev1.Node, p *corev1.Pod) error {
	//       leaseName := fmt.Sprintf("hami-node-%s", n.Name)
	//       err := nodelock.ReleaseNodeLock(leaseName, string(p.UID))
	//       if err != nil {
	//           klog.Warningf("Failed to release lock for node %s: %v", n.Name, err)
	//       }
	//       return nil
	//   }
	//
	// 调用时机：
	// - 调度成功后：Bind 完成后释放
	// - 调度失败后：Filter/Score 失败后释放
	// - 超时后：自动过期（Lease 机制）
	//
	// 参数：
	// - n: 节点对象
	// - p: Pod 对象（用于验证锁的持有者）
	//
	// 返回值：
	// - error: 释放锁失败时返回错误（通常可以忽略）
	ReleaseNodeLock(n *corev1.Node, p *corev1.Pod) error
}

type MigTemplate struct {
	Name   string `yaml:"name"`
	Core   int32  `yaml:"core"`
	Memory int32  `yaml:"memory"`
	Count  int32  `yaml:"count"`
}

type MigTemplateUsage struct {
	Name   string `json:"name,omitempty"`
	Core   int32  `json:"core,omitempty"`
	Memory int32  `json:"memory,omitempty"`
	InUse  bool   `json:"inuse,omitempty"`
}

type Geometry []MigTemplate

type MIGS []MigTemplateUsage

type MigInUse struct {
	Index     int32
	UsageList MIGS
}

type AllowedMigGeometries struct {
	Models     []string   `yaml:"models"`
	Geometries []Geometry `yaml:"allowedGeometries"`
}

type DeviceUsage struct {
	ID          string
	Index       uint
	Used        int32
	Count       int32
	Usedmem     int32
	Totalmem    int32
	Totalcore   int32
	Usedcores   int32
	Mode        string
	MigTemplate []Geometry
	MigUsage    MigInUse
	Numa        int
	Type        string
	Health      bool
	PodInfos    []*PodInfo
	CustomInfo  map[string]any
}

type DeviceInfo struct {
	ID              string          `json:"id,omitempty"`
	Index           uint            `json:"index,omitempty"`
	Count           int32           `json:"count,omitempty"`
	Devmem          int32           `json:"devmem,omitempty"`
	Devcore         int32           `json:"devcore,omitempty"`
	Type            string          `json:"type,omitempty"`
	Numa            int             `json:"numa,omitempty"`
	Mode            string          `json:"mode,omitempty"`
	MIGTemplate     []Geometry      `json:"migtemplate,omitempty"`
	Health          bool            `json:"health,omitempty"`
	DeviceVendor    string          `json:"devicevendor,omitempty"`
	CustomInfo      map[string]any  `json:"custominfo,omitempty"`
	DevicePairScore DevicePairScore `json:"devicepairscore,omitempty"`
}

type DevicePairScores []DevicePairScore
type DevicePairScore struct {
	ID     string         `json:"uuid,omitempty"`
	Scores map[string]int `json:"score,omitempty"`
}

type NodeInfo struct {
	ID      string
	Node    *corev1.Node
	Devices map[string][]DeviceInfo
}

type ResourceNames struct {
	ResourceCountName  string
	ResourceMemoryName string
	ResourceCoreName   string
}

type ContainerDevice struct {
	// TODO current Idx cannot use, because EncodeContainerDevices method not encode this filed.
	Idx        int
	UUID       string
	Type       string
	Usedmem    int32
	Usedcores  int32
	CustomInfo map[string]any
}

type ContainerDeviceRequest struct {
	Nums             int32
	Type             string
	Memreq           int32
	MemPercentagereq int32
	Coresreq         int32
}

type ContainerDevices []ContainerDevice
type ContainerDeviceRequests map[string]ContainerDeviceRequest

// type ContainerAllDevices map[string]ContainerDevices.
type PodSingleDevice []ContainerDevices
type PodDeviceRequests []ContainerDeviceRequests
type PodDevices map[string]PodSingleDevice

const (
	// OneContainerMultiDeviceSplitSymbol this is when one container use multi device, use : symbol to join device info.
	OneContainerMultiDeviceSplitSymbol = ":"

	// OnePodMultiContainerSplitSymbol this is when one pod having multi container and more than one container use device, use ; symbol to join device info.
	OnePodMultiContainerSplitSymbol = ";"
)

var (
	GPUSchedulerPolicy string
	// 存储"待分配设备"的注解名称
	// 例如：NVIDIA: "hami.io/vgpu-devices-to-allocate"
	InRequestDevices map[string]string
	// 存储"已分配设备"的注解名称
	// 例如：NVIDIA: "hami.io/vgpu-devices-allocated"
	//      AMDGPU: "hami.io/amd-devices-allocated"
	SupportDevices  map[string]string
	DevicesMap      map[string]Devices
	DevicesToHandle []string
)

func init() {
	InRequestDevices = make(map[string]string)
	SupportDevices = make(map[string]string)
}

func GetDevices() map[string]Devices {
	return DevicesMap
}

func DecodeNodeDevices(str string) ([]*DeviceInfo, error) {
	if !strings.Contains(str, OneContainerMultiDeviceSplitSymbol) {
		return []*DeviceInfo{}, errors.New("node annotations not decode successfully")
	}
	tmp := strings.Split(str, OneContainerMultiDeviceSplitSymbol)
	var retval []*DeviceInfo
	for _, val := range tmp {
		if strings.Contains(val, ",") {
			items := strings.Split(val, ",")
			if len(items) == 7 || len(items) == 9 {
				count, _ := strconv.ParseInt(items[1], 10, 32)
				devmem, _ := strconv.ParseInt(items[2], 10, 32)
				devcore, _ := strconv.ParseInt(items[3], 10, 32)
				health, _ := strconv.ParseBool(items[6])
				numa, _ := strconv.Atoi(items[5])
				mode := "hami-core"
				index := 0
				if len(items) == 9 {
					index, _ = strconv.Atoi(items[7])
					mode = items[8]
				}
				count32, err := safecast.Convert[int32](count)
				if err != nil {
					return []*DeviceInfo{}, errors.New("node annotations not decode successfully")
				}
				devmem32, err := safecast.Convert[int32](devmem)
				if err != nil {
					return []*DeviceInfo{}, errors.New("node annotations not decode successfully")
				}
				devcore32, err := safecast.Convert[int32](devcore)
				if err != nil {
					return []*DeviceInfo{}, errors.New("node annotations not decode successfully")
				}
				i := DeviceInfo{
					ID:      items[0],
					Count:   count32,
					Devmem:  devmem32,
					Devcore: devcore32,
					Type:    items[4],
					Numa:    numa,
					Health:  health,
					Mode:    mode,
					Index:   uint(index),
				}
				retval = append(retval, &i)
			} else {
				return []*DeviceInfo{}, errors.New("node annotations not decode successfully")
			}
		}
	}
	return retval, nil
}

func DecodePairScores(pairScores string) (*DevicePairScores, error) {
	devicePairScores := &DevicePairScores{}
	if err := json.Unmarshal([]byte(pairScores), devicePairScores); err != nil {
		return nil, err
	}
	return devicePairScores, nil
}

func EncodeNodeDevices(dlist []*DeviceInfo) string {
	builder := strings.Builder{}
	for _, val := range dlist {
		builder.WriteString(val.ID)
		builder.WriteString(",")
		builder.WriteString(strconv.FormatInt(int64(val.Count), 10))
		builder.WriteString(",")
		builder.WriteString(strconv.Itoa(int(val.Devmem)))
		builder.WriteString(",")
		builder.WriteString(strconv.Itoa(int(val.Devcore)))
		builder.WriteString(",")
		builder.WriteString(val.Type)
		builder.WriteString(",")
		builder.WriteString(strconv.Itoa(val.Numa))
		builder.WriteString(",")
		builder.WriteString(strconv.FormatBool(val.Health))
		builder.WriteString(",")
		builder.WriteString(strconv.Itoa(int(val.Index)))
		builder.WriteString(",")
		builder.WriteString(val.Mode)
		builder.WriteString(OneContainerMultiDeviceSplitSymbol)
		//tmp += val.ID + "," + strconv.FormatInt(int64(val.Count), 10) + "," + strconv.Itoa(int(val.Devmem)) + "," + strconv.Itoa(int(val.Devcore)) + "," + val.Type + "," + strconv.Itoa(val.Numa) + "," + strconv.FormatBool(val.Health) + "," + strconv.Itoa(val.Index) + OneContainerMultiDeviceSplitSymbol
	}
	tmp := builder.String()
	klog.V(5).Infof("Encoded node Devices: %s", tmp)
	return tmp
}

// MarshalNodeDevices will only marshal general information, customInfo is neglected.
func MarshalNodeDevices(dlist []*DeviceInfo) string {
	devAnnos := []*DeviceInfo{}
	for _, val := range dlist {
		devAnnos = append(devAnnos, &DeviceInfo{
			ID:      val.ID,
			Count:   val.Count,
			Devmem:  val.Devmem,
			Devcore: val.Devcore,
			Type:    val.Type,
			Numa:    val.Numa,
			Health:  val.Health,
			Index:   val.Index,
			Mode:    val.Mode,
		})
	}
	data, err := json.Marshal(devAnnos)
	if err != nil {
		return ""
	}
	return string(data)
}

func UnMarshalNodeDevices(str string) ([]*DeviceInfo, error) {
	var dlist []*DeviceInfo
	err := json.Unmarshal([]byte(str), &dlist)
	return dlist, err
}

func EncodeContainerDevices(cd ContainerDevices) string {
	tmp := ""
	for _, val := range cd {
		tmp += val.UUID + "," + val.Type + "," + strconv.Itoa(int(val.Usedmem)) + "," + strconv.Itoa(int(val.Usedcores)) + OneContainerMultiDeviceSplitSymbol
	}
	klog.Infof("Encoded container Devices: %s", tmp)
	return tmp
	//return strings.Join(cd, ",")
}

func EncodeContainerDeviceType(cd ContainerDevices, t string) string {
	tmp := ""
	for _, val := range cd {
		if strings.Compare(val.Type, t) == 0 {
			tmp += val.UUID + "," + val.Type + "," + strconv.Itoa(int(val.Usedmem)) + "," + strconv.Itoa(int(val.Usedcores))
		}
		tmp += OneContainerMultiDeviceSplitSymbol
	}
	klog.Infof("Encoded container Certain Device type: %s->%s", t, tmp)
	return tmp
}

func EncodePodSingleDevice(pd PodSingleDevice) string {
	res := ""
	for _, ctrdevs := range pd {
		res = res + EncodeContainerDevices(ctrdevs)
		res = res + OnePodMultiContainerSplitSymbol
	}
	klog.Infof("Encoded pod single devices %s", res)
	return res
}

func EncodePodDevices(checklist map[string]string, pd PodDevices) map[string]string {
	res := map[string]string{}
	for devType, cd := range pd {
		klog.Infoln("devtype=", devType)
		res[checklist[devType]] = EncodePodSingleDevice(cd)
	}
	klog.Infof("Encoded pod Devices %s\n", res)
	return res
}

func DecodeContainerDevices(str string) (ContainerDevices, error) {
	if len(str) == 0 {
		return ContainerDevices{}, nil
	}
	cd := strings.Split(str, OneContainerMultiDeviceSplitSymbol)
	contdev := ContainerDevices{}
	tmpdev := ContainerDevice{}
	klog.V(5).Infof("Start to decode container device %s", str)
	for _, val := range cd {
		if strings.Contains(val, ",") {
			//fmt.Println("cd is ", val)
			tmpstr := strings.Split(val, ",")
			if len(tmpstr) < 4 {
				return ContainerDevices{}, fmt.Errorf("pod annotation format error; information missing, please do not use nodeName field in task")
			}
			tmpdev.UUID = tmpstr[0]
			tmpdev.Type = tmpstr[1]
			devmem, _ := strconv.ParseInt(tmpstr[2], 10, 32)
			tmpdev.Usedmem = int32(devmem)
			devcores, _ := strconv.ParseInt(tmpstr[3], 10, 32)
			tmpdev.Usedcores = int32(devcores)
			contdev = append(contdev, tmpdev)
		}
	}
	klog.V(5).Infof("Finished decoding container devices. Total devices: %d", len(contdev))
	return contdev, nil
}

func DecodePodDevices(checklist map[string]string, annos map[string]string) (PodDevices, error) {
	klog.V(5).Infof("checklist is [%+v], annos is [%+v]", checklist, annos)
	if len(annos) == 0 {
		return PodDevices{}, nil
	}
	pd := make(PodDevices)
	for devID, devs := range checklist {
		str, ok := annos[devs]
		if !ok {
			continue
		}
		pd[devID] = make(PodSingleDevice, 0)
		for s := range strings.SplitSeq(str, OnePodMultiContainerSplitSymbol) {
			cd, err := DecodeContainerDevices(s)
			if err != nil {
				return PodDevices{}, nil
			}
			if len(cd) == 0 {
				continue
			}
			pd[devID] = append(pd[devID], cd)
		}
	}
	klog.V(5).InfoS("Decoded pod annos", "poddevices", pd)
	return pd, nil
}

func PlatternMIG(n *MigInUse, templates []Geometry, templateIdx int) {
	var err error
	for _, val := range templates[templateIdx] {
		count := 0
		for count < int(val.Count) {
			n.Index, err = safecast.Convert[int32](templateIdx)
			if err != nil {
				continue
			}
			n.UsageList = append(n.UsageList, MigTemplateUsage{
				Name:   val.Name,
				Memory: val.Memory,
				Core:   val.Core,
				InUse:  false,
			})
			count++
		}
	}
}

func GetDevicesUUIDList(infos []*DeviceInfo) []string {
	uuids := make([]string, 0)
	for _, info := range infos {
		uuids = append(uuids, info.ID)
	}
	return uuids
}

// CheckHealth 通用的设备健康检查实现（基于 HandshakeAnnos 握手机制）
// 这是为其他设备类型（如 Hygon DCU）提供的默认实现
//
// 注意：NVIDIA 设备有自己的 CheckHealth 实现，不使用这个函数！
// - NVIDIA: 基于 Status.Allocatable 数量对比（pkg/device/nvidia/device.go:223）
// - Hygon DCU: 调用此函数，使用 HandshakeAnnos 握手（pkg/device/hygon/device.go:183）
// - AMD: 简单返回 (true, true)，不做检查（pkg/device/amd/device.go:127）
//
// 握手机制工作原理：
// 1. Scheduler 定期检查节点（每 15 秒）
// 2. 如果注解为空或其他值，打上 "Requesting_时间戳"
// 3. Device Plugin 看到 "Requesting" 后，重新上报设备信息并更新注解
// 4. 如果 60 秒内没响应，认为不健康
// 5. 如果注解为 "Deleted"，表示设备已下线，跳过检查
//
// HandshakeAnnos 的三种状态：
// - 空或其他值: 首次检查或需要重新握手 → 打上 "Requesting_时间戳"，返回 (true, true)
// - "Requesting_时间戳": 等待 Device Plugin 响应 → 检查是否超时 60 秒
//   - 未超时: 返回 (true, false) - 健康但不更新
//   - 超时: 返回 (false, false) - 不健康，触发清理
//
// - "Deleted": 设备已下线 → 返回 (true, false) - 暂时健康但不更新，避免重复清理
//
// 为什么需要握手机制？
// - 协调 Scheduler 和 Device Plugin 之间的通信
// - 触发 Device Plugin 重新上报设备信息
// - 检测 Device Plugin 是否响应（超时检测）
// - 避免重复清理（通过 "Deleted" 状态）
//
// 参数：
// - devType: 设备类型（如 "DCU"）
// - node: Kubernetes 节点对象
//
// 返回值：
// - 第一个 bool: 设备是否健康
// - 第二个 bool: 是否需要更新设备信息
func CheckHealth(devType string, node *corev1.Node) (bool, bool) {
	handshake := node.Annotations[util.HandshakeAnnos[devType]]
	if strings.Contains(handshake, "Requesting") {
		// 情况 1: 正在等待 Device Plugin 响应，检查是否超时（60秒）
		formertime, _ := time.Parse(time.DateTime, strings.Split(handshake, "_")[1])
		return time.Now().Before(formertime.Add(time.Second * 60)), false
	} else if strings.Contains(handshake, "Deleted") {
		// 情况 2: 设备已标记删除，返回健康但不更新（避免重复清理）
		return true, false
	} else {
		// 情况 3: 首次检查或需要重新握手，打上 "Requesting" 时间戳
		_, ok := util.HandshakeAnnos[devType]
		if ok {
			tmppat := make(map[string]string)
			tmppat[util.HandshakeAnnos[devType]] = "Requesting_" + time.Now().Format(time.DateTime)
			klog.V(5).InfoS("New timestamp for annotation", "nodeName", node.Name, "annotationKey", util.HandshakeAnnos[devType], "annotationValue", tmppat[util.HandshakeAnnos[devType]])
			n, err := util.GetNode(node.Name)
			if err != nil {
				klog.ErrorS(err, "Failed to get node", "nodeName", node.Name)
				return true, false
			}
			klog.V(5).InfoS("Patching node annotations", "nodeName", node.Name, "annotations", tmppat)
			if err := util.PatchNodeAnnotations(n, tmppat); err != nil {
				klog.ErrorS(err, "Failed to patch node annotations", "nodeName", node.Name)
			}
		}
		return true, true
	}
}

// Enhanced ExtractMigTemplatesFromUUID with error handling.
func ExtractMigTemplatesFromUUID(uuid string) (int, int, error) {
	parts := strings.Split(uuid, "[")
	if len(parts) < 2 {
		return -1, -1, fmt.Errorf("invalid UUID format: missing '[' delimiter")
	}

	tmp := parts[1]
	parts = strings.Split(tmp, "]")
	if len(parts) < 2 {
		return -1, -1, fmt.Errorf("invalid UUID format: missing ']' delimiter")
	}

	tmp = parts[0]
	parts = strings.Split(tmp, "-")
	if len(parts) < 2 {
		return -1, -1, fmt.Errorf("invalid UUID format: missing '-' delimiter")
	}

	templateIdx, err := strconv.Atoi(parts[0])
	if err != nil {
		return -1, -1, fmt.Errorf("invalid template index: %v", err)
	}

	pos, err := strconv.Atoi(parts[1])
	if err != nil {
		return -1, -1, fmt.Errorf("invalid position: %v", err)
	}

	return templateIdx, pos, nil
}

// 获取 pod 申请的资源/设备
func Resourcereqs(pod *corev1.Pod) (counts PodDeviceRequests) {
	// []map[string]ContainerDeviceRequest
	counts = make(PodDeviceRequests, len(pod.Spec.Containers))
	klog.V(4).InfoS("Processing resource requirements",
		"pod", klog.KObj(pod),
		"containerCount", len(pod.Spec.Containers))
	// Count Nvidia GPU
	cnt := int32(0)
	for i := range pod.Spec.Containers {
		devices := GetDevices()
		counts[i] = make(ContainerDeviceRequests)
		klog.V(5).InfoS("Processing container resources",
			"pod", klog.KObj(pod),
			"containerIndex", i,
			"containerName", pod.Spec.Containers[i].Name)
		for idx, val := range devices {
			request := val.GenerateResourceRequests(&pod.Spec.Containers[i])
			if request.Nums > 0 {
				cnt += request.Nums
				counts[i][idx] = request
			}
		}
	}
	if cnt == 0 {
		klog.V(4).InfoS("No device requests found", "pod", klog.KObj(pod))
	} else {
		klog.V(4).InfoS("Resource requirements collected", "pod", klog.KObj(pod), "requests", counts)
	}
	return counts
}

func CheckUUID(annos map[string]string, id, useKey, noUseKey, deviceType string) bool {
	userUUID, ok := annos[useKey]
	if ok {
		klog.V(5).Infof("check uuid for %s user uuid [%s], device id is %s", deviceType, userUUID, id)
		// use , symbol to connect multiple uuid
		userUUIDs := strings.Split(userUUID, ",")
		return slices.Contains(userUUIDs, id)
	}

	noUserUUID, ok := annos[noUseKey]
	if ok {
		klog.V(5).Infof("check uuid for %s not user uuid [%s], device id is %s", deviceType, noUserUUID, id)
		// use , symbol to connect multiple uuid
		noUserUUIDs := strings.Split(noUserUUID, ",")
		return !slices.Contains(noUserUUIDs, id)
	}
	return true
}
