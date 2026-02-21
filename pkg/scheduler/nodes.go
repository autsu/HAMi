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

package scheduler

import (
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/policy"
)

// NodeUsage 节点的设备使用情况
// 用于调度决策时的节点状态表示
//
// 字段说明：
// - Node: Kubernetes 节点对象
// - Devices: 节点上所有设备的使用情况列表
type NodeUsage struct {
	Node    *corev1.Node
	Devices policy.DeviceUsageList
}

// nodeManager 节点管理器
// 负责维护集群中所有节点的设备信息
//
// 字段说明：
// - nodes: 节点 ID 到节点信息的映射
// - mutex: 读写锁，保护并发访问
//
// 为什么需要读写锁？
// - 多个 goroutine 可能同时访问节点信息
// - 读操作（GetNode、ListNodes）可以并发
// - 写操作（addNode、rmNode）需要独占
// - 读写锁提高并发性能
type nodeManager struct {
	nodes map[string]*device.NodeInfo
	mutex sync.RWMutex
}

// newNodeManager 创建节点管理器实例
//
// 返回值：
// - *nodeManager: 初始化完成的节点管理器
func newNodeManager() *nodeManager {
	return &nodeManager{
		nodes: make(map[string]*device.NodeInfo),
	}
}

// addNode 添加或更新节点的设备信息
// 这是节点注册的核心方法
//
// 处理逻辑：
// 1. 检查节点信息是否有效（非空且有设备）
// 2. 如果节点已存在，合并设备信息
// 3. 如果节点不存在，直接添加
//
// 为什么要合并设备？
// - 一个节点可能有多种设备类型（GPU、NPU 等）
// - 不同设备类型可能在不同时间注册
// - 合并确保节点信息的完整性
//
// 为什么更新 Node 对象？
// - Node 对象包含标签、污点等调度相关信息
// - 这些信息可能会变化（例如添加标签）
// - 保持最新状态确保调度决策正确
//
// 参数：
// - nodeID: 节点名称
// - nodeInfo: 节点的设备信息
func (m *nodeManager) addNode(nodeID string, nodeInfo *device.NodeInfo) {
	if nodeInfo == nil || len(nodeInfo.Devices) == 0 {
		return
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	_, ok := m.nodes[nodeID]
	if ok {
		if len(nodeInfo.Devices) > 0 {
			for vendor := range nodeInfo.Devices {
				m.nodes[nodeID].Devices[vendor] = nodeInfo.Devices[vendor]
			}
		}
		m.nodes[nodeID].Node = nodeInfo.Node
	} else {
		m.nodes[nodeID] = nodeInfo
	}
}

// rmNodeDevices 删除节点的特定设备类型
// 用于设备不健康或被移除时的清理
//
// 处理逻辑：
// 1. 删除指定设备类型的信息
// 2. 如果节点没有任何设备了，删除整个节点
//
// 为什么要分设备类型删除？
// - 节点可能有多种设备类型
// - 某种设备不健康不影响其他设备
// - 细粒度控制，避免误删
//
// 何时调用？
// - 设备健康检查失败
// - Device Plugin 停止上报设备
// - 设备被手动下线
//
// 参数：
// - nodeID: 节点名称
// - deviceVendor: 设备类型（如 "NVIDIA"）
func (m *nodeManager) rmNodeDevices(nodeID string, deviceVendor string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	nodeInfo := m.nodes[nodeID]
	if nodeInfo == nil {
		return
	}
	// 删除 node 上的设备信息
	delete(m.nodes[nodeID].Devices, deviceVendor)
	if len(m.nodes[nodeID].Devices) == 0 {
		delete(m.nodes, nodeID)
	}
	klog.InfoS("Removing device from node", "nodeName", nodeID, "deviceVendor", deviceVendor)
}

// rmNode 删除节点的所有信息
// 用于节点被删除时的清理
//
// 何时调用？
// - 节点从集群中删除（Informer 的 DeleteFunc）
// - 节点长时间不可达
// - 管理员手动删除节点
//
// 为什么需要这个方法？
// - 避免内存泄漏（保留已删除节点的信息）
// - 确保调度决策基于有效节点
// - 配合 Informer 的事件机制
//
// 参数：
// - nodeID: 节点名称
func (m *nodeManager) rmNode(nodeID string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if _, ok := m.nodes[nodeID]; ok {
		delete(m.nodes, nodeID)
		klog.InfoS("Removing node from nodeManager", "nodeName", nodeID)
	}
}

// GetNode 获取指定节点的信息
// 用于调度决策时查询节点状态
//
// 返回值：
// - *device.NodeInfo: 节点信息（包含设备列表）
// - error: 节点不存在时返回错误
//
// 为什么使用读锁？
// - 只读操作，不修改数据
// - 允许多个 goroutine 并发读取
// - 提高性能
func (m *nodeManager) GetNode(nodeID string) (*device.NodeInfo, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if n, ok := m.nodes[nodeID]; ok {
		return n, nil
	}
	return &device.NodeInfo{}, fmt.Errorf("node %v not found", nodeID)
}

// ListNodes 列出所有节点的信息
// 用于全局视图和监控指标
//
// 为什么要深拷贝？
// - 避免调用者修改内部数据
// - 保证数据的一致性和安全性
// - 调用者可以自由使用返回的数据
//
// 深拷贝的内容：
// - Node 对象（DeepCopy）
// - Devices 映射（逐个拷贝）
// - 设备列表（copy 函数）
//
// 为什么跳过 nil 节点？
// - 防止并发修改导致的数据不一致
// - 避免返回无效数据
// - 记录警告日志便于排查问题
//
// 返回值：
// - map[string]*device.NodeInfo: 节点 ID 到节点信息的映射（深拷贝）
// - error: 处理过程中的错误
func (m *nodeManager) ListNodes() (map[string]*device.NodeInfo, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	nodesCopy := make(map[string]*device.NodeInfo, len(m.nodes))
	for nodeID, nodeInfo := range m.nodes {
		if nodeInfo == nil || nodeInfo.Node == nil {
			klog.Warningf("ListNodes nodes copy step skip node(%s) because of nil NodeInfo or NodeInfo.Node", nodeID)
			continue
		}
		nodeInfoCopy := &device.NodeInfo{
			ID:      nodeInfo.ID,
			Node:    nodeInfo.Node.DeepCopy(),
			Devices: make(map[string][]device.DeviceInfo),
		}
		for k, v := range nodeInfo.Devices {
			nodeInfoCopy.Devices[k] = make([]device.DeviceInfo, len(v))
			copy(nodeInfoCopy.Devices[k], v)
		}
		nodesCopy[nodeID] = nodeInfoCopy
	}
	return nodesCopy, nil
}
