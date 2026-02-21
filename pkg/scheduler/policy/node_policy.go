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

package policy

import (
	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/util"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// NodeScore 节点的调度分数结构
// 用于节点级别的调度决策
//
// 字段说明：
// - NodeID: 节点名称
// - Node: 节点对象（包含标签、污点等信息）
// - Devices: 分配给 Pod 的设备列表（按设备类型分组）
// - Score: 节点的匹配分数（用于排序）
type NodeScore struct {
	NodeID  string
	Node    *corev1.Node
	Devices device.PodDevices
	// Score recode every node all device user/allocate score
	Score float32
}

// NodeScoreList 节点分数列表
// 实现了 sort.Interface，支持按策略排序
//
// 字段说明：
// - NodeList: 节点列表
// - Policy: 调度策略（binpack 或 spread）
type NodeScoreList struct {
	NodeList []*NodeScore
	Policy   string
}

// Len 返回节点列表长度
// sort.Interface 的必需方法
func (l NodeScoreList) Len() int {
	return len(l.NodeList)
}

// Swap 交换两个节点的位置
// sort.Interface 的必需方法
func (l NodeScoreList) Swap(i, j int) {
	l.NodeList[i], l.NodeList[j] = l.NodeList[j], l.NodeList[i]
}

// Less 定义节点的排序规则
// 这是节点级别调度策略的实现
//
// Spread 策略（分散）：
// - 优先选择空闲资源多的节点（分数高的排前面）
// - 目标：均衡集群负载，提高可用性
//
// Binpack 策略（装箱）- 默认：
// - 优先选择已使用资源多的节点（分数低的排前面）
// - 目标：集中使用节点，节省成本（可关闭空闲节点）
//
// 为什么默认是 Binpack？
// - 云环境中可以关闭空闲节点节省成本
// - 减少节点数量，降低管理复杂度
// - 提高单节点利用率
//
// 何时使用 Spread？
// - 需要高可用性（分散风险）
// - 单节点性能有瓶颈
// - 希望减少资源竞争
//
// sort.Interface 的必需方法
func (l NodeScoreList) Less(i, j int) bool {
	if l.Policy == util.NodeSchedulerPolicySpread.String() {
		return l.NodeList[i].Score > l.NodeList[j].Score
	}
	// default policy is Binpack
	return l.NodeList[i].Score < l.NodeList[j].Score
}

//+scheduler:2.2.3
// OverrideScore 计算节点的最终分数
// 在基础分数上叠加设备特定的分数
//
// 处理流程：
// 1. 遍历分配的设备（按设备类型）
// 2. 调用设备的 ScoreNode 方法计算设备分数
// 3. 累加到节点的基础分数上
//
// 为什么需要 Override？
// - ComputeDefaultScore 只考虑了资源使用率
// - 不同设备类型有特定的调度需求
// - 例如：NUMA 亲和性、设备型号偏好、自定义策略
//
// ScoreNode 的作用：
// - 由具体设备实现（NVIDIA、AMD 等）
// - 考虑设备特定的因素
// - 返回额外的分数调整值
//
// previous 参数的作用：
// - 保存分配前的设备状态快照
// - 用于计算分配的影响
// - 支持基于变化量的打分
//
// 参数：
// - previous: 分配前的设备状态快照
// - policy: 节点调度策略
//
// 副作用：
// - 更新 ns.Score 字段
func (ns *NodeScore) OverrideScore(previous []*device.DeviceUsage, policy string) {
	// current user having request resource
	devScore := float32(0)
	for idx, val := range ns.Devices {
		devScore += device.GetDevices()[idx].ScoreNode(ns.Node, val, previous, policy)
	}
	ns.Score += devScore
	klog.V(2).Infof("node %s default score is %f, computer override score is %f", ns.NodeID, ns.Score-devScore, ns.Score)
}

// SnapshotDevice 创建设备状态的快照
// 用于保存分配前的状态
//
// 功能说明：
// - 深拷贝每个设备的 DeviceUsage 结构
// - 保存当前的使用量、分数等信息
// - 用于 OverrideScore 计算分配影响
//
// 为什么需要快照？
// - fitInDevices 会修改设备的使用量
// - OverrideScore 需要对比分配前后的变化
// - 快照提供了"分配前"的参考状态
//
// 深拷贝的重要性：
// - 避免引用同一个对象
// - 修改不会影响快照
// - 保证数据的独立性
//
// 参数：
// - devices: 设备使用列表
//
// 返回值：
// - []*device.DeviceUsage: 设备状态快照
func (ns *NodeScore) SnapshotDevice(devices DeviceUsageList) []*device.DeviceUsage {
	snapshot := []*device.DeviceUsage{}
	for _, val := range devices.DeviceLists {
		tmp := *val.Device
		snapshot = append(snapshot, &tmp)
	}
	return snapshot
}

//+scheduler:2.2.4
// ComputeDefaultScore 计算节点的基础分数
// 基于当前资源使用率
//
// 计算公式：
// score = weight × (useScore + coreScore + memScore)
// 其中：
// - useScore = used / total              // 设备数量使用率
// - coreScore = usedCore / totalCore     // 算力使用率
// - memScore = usedMem / totalMem        // 显存使用率
//
// 为什么是三个维度？
// - 全面反映节点的资源使用情况
// - 避免单一维度的偏差
// - 与设备级别的打分保持一致
//
// 分数的含义：
// - 分数越高 = 资源使用率越高
// - Binpack 策略：优先选择分数高的节点（集中使用）
// - Spread 策略：优先选择分数低的节点（分散负载）
//
// 为什么要遍历所有设备？
// - 累加节点上所有设备的资源
// - 得到节点级别的总量和使用量
// - 用于计算整体使用率
//
// 参数：
// - devices: 节点的设备使用列表
//
// 副作用：
// - 更新 ns.Score 字段
func (ns *NodeScore) ComputeDefaultScore(devices DeviceUsageList) {
	used, usedCore, usedMem := int32(0), int32(0), int32(0)
	for _, device := range devices.DeviceLists {
		used += device.Device.Used
		usedCore += device.Device.Usedcores
		usedMem += device.Device.Usedmem
	}
	klog.V(2).Infof("node %s used %d, usedCore %d, usedMem %d,", ns.NodeID, used, usedCore, usedMem)

	total, totalCore, totalMem := int32(0), int32(0), int32(0)
	for _, deviceLists := range devices.DeviceLists {
		total += deviceLists.Device.Count
		totalCore += deviceLists.Device.Totalcore
		totalMem += deviceLists.Device.Totalmem
	}
	useScore := float32(used) / float32(total)
	coreScore := float32(usedCore) / float32(totalCore)
	memScore := float32(usedMem) / float32(totalMem)
	ns.Score = float32(util.Weight) * (useScore + coreScore + memScore)
	klog.V(2).Infof("node %s computer default score is %f", ns.NodeID, ns.Score)
}
