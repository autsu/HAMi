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
	"sort"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/device/common"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/config"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/policy"
	"github.com/Project-HAMi/HAMi/pkg/util"
)

// viewStatus 打印节点设备状态的调试信息
// 用于排查调度问题
//
// 功能说明：
// - 遍历节点的所有设备
// - 打印每个设备的详细信息
// - 使用 V(5) 级别日志（详细调试）
//
// 何时使用？
// - 调度失败时查看设备状态
// - 验证设备使用量是否正确
// - 排查设备分配问题
//
// 参数：
// - usage: 节点的设备使用情况
func viewStatus(usage NodeUsage) {
	klog.V(5).Info("devices status")
	for _, val := range usage.Devices.DeviceLists {
		klog.V(5).InfoS("device status", "device id", val.Device.ID, "device detail", val)
	}
}

// getNodeResources 从节点使用情况中提取指定类型的设备列表
// 这是一个辅助函数，用于过滤设备类型
//
// 功能说明：
// - 遍历节点的所有设备
// - 根据设备类型（Type）进行过滤
// - 返回匹配的设备使用情况列表
//
// 为什么需要类型过滤？
// - 一个节点可能有多种设备（GPU、NPU、DCU 等）
// - 不同设备的调度逻辑不同
// - 传递给设备特定的 Fit 方法
//
// 类型匹配使用 Contains：
// - 支持模糊匹配（例如 "NVIDIA" 匹配 "NVIDIA-A100"）
// - 兼容设备型号的变化
// - 灵活的设备分类
//
// 参数：
// - list: 节点的设备使用情况
// - t: 设备类型字符串
//
// 返回值：
// - []*device.DeviceUsage: 匹配的设备列表
func getNodeResources(list NodeUsage, t string) []*device.DeviceUsage {
	l := []*device.DeviceUsage{}
	for _, val := range list.Devices.DeviceLists {
		if strings.Contains(val.Device.Type, t) {
			l = append(l, val.Device)
		}
	}
	return l
}

// +scheduler:2.2.1
// fitInDevices 检查节点是否能满足容器的设备需求
// 这是设备匹配的核心逻辑
//
// 处理流程：
// 1. 计算设备分数：为节点上的所有设备计算匹配分数
// 2. 排序设备列表：根据调度策略（Binpack/Spread）排序
// 3. 设备类型匹配：遍历每种设备类型的请求
// 4. 调用设备 Fit 方法：由具体设备实现判断是否满足
// 5. 更新设备使用量：调用 AddResourceUsage 更新设备状态
// 6. 累加资源统计：记录总资源和空闲资源
//
// 为什么要先计算分数再排序？
// - 分数反映了设备的"适合程度"
// - Binpack 策略：优先使用已分配的设备（分数高 = 已使用多）
// - Spread 策略：优先使用空闲设备（分数高 = 空闲多）
// - 排序后按顺序尝试，提高匹配成功率
//
// 设备分数的计算：
// - 考虑设备数量、显存、算力三个维度
// - 加权求和：score = weight × (used/total + usedCore/totalCore + usedMem/totalMem)
// - 分数越高表示资源使用越集中（Binpack）或越分散（Spread）
//
// 为什么需要 AddResourceUsage？
// - Fit 方法只判断是否满足，不修改状态
// - AddResourceUsage 更新设备的使用量
// - 支持同一个 Pod 的多个容器依次分配设备
// - 确保后续容器看到的是更新后的状态
//
// 为什么要检查设备数量？
// - 快速失败：请求的设备数超过节点总数，直接返回
// - 避免无效计算，提高性能
// - 返回明确的失败原因
//
// 参数：
// - node: 节点的设备使用情况
// - requests: 容器的设备请求列表
// - pod: 待调度的 Pod
// - nodeInfo: 节点的完整信息
// - devinput: 输出参数，存储分配结果
//
// 返回值：
// - bool: true 表示满足需求，false 表示不满足
// - string: 失败原因（成功时为空）
func fitInDevices(node *NodeUsage, requests device.ContainerDeviceRequests, pod *corev1.Pod, nodeInfo *device.NodeInfo, devinput *device.PodDevices) (bool, string) {
	//devmap := make(map[string]device.ContainerDevices)
	devs := device.ContainerDevices{}
	total, totalCore, totalMem := int32(0), int32(0), int32(0)
	free, freeCore, freeMem := int32(0), int32(0), int32(0)
	sums := 0
	// computer all device score for one node
	for index := range node.Devices.DeviceLists {
		node.Devices.DeviceLists[index].ComputeScore(requests)
	}
	//This loop is for requests for different devices
	for _, k := range requests {
		sums += int(k.Nums)
		if int(k.Nums) > len(node.Devices.DeviceLists) {
			klog.V(5).InfoS(common.NodeInsufficientDevice, "pod", klog.KObj(pod), "request devices nums", k.Nums, "node device nums", len(node.Devices.DeviceLists))
			return false, common.NodeInsufficientDevice
		}
		sort.Sort(node.Devices)
		_, ok := device.GetDevices()[k.Type]
		if !ok {
			return false, "Device type not found"
		}
		fit, tmpDevs, reason := device.GetDevices()[k.Type].Fit(getNodeResources(*node, k.Type), k, pod, nodeInfo, devinput)
		if fit {
			for idx, val := range tmpDevs[k.Type] {
				for nidx, v := range node.Devices.DeviceLists {
					//bc node.Devices has been sorted, so we should find out the correct device
					if v.Device.ID != val.UUID {
						continue
					}
					total += v.Device.Count
					totalCore += v.Device.Totalcore
					totalMem += v.Device.Totalmem
					free += v.Device.Count - v.Device.Used
					freeCore += v.Device.Totalcore - v.Device.Usedcores
					freeMem += v.Device.Totalmem - v.Device.Usedmem
					err := device.GetDevices()[k.Type].AddResourceUsage(pod, node.Devices.DeviceLists[nidx].Device, &tmpDevs[k.Type][idx])
					if err != nil {
						klog.Errorf("AddResourceUsage failed:%s", err.Error())
						return false, "AddResourceUsage failed"
					}
					klog.V(5).Infoln("After AddResourceUsage:", node.Devices.DeviceLists[nidx].Device)
				}
			}
			devs = append(devs, tmpDevs[k.Type]...)
		} else {
			return false, reason
		}
		(*devinput)[k.Type] = append((*devinput)[k.Type], devs)
	}
	return true, ""
}

// +scheduler:2.2
// calcScore 计算每个节点的调度分数
// 这是 Filter 流程的核心：为每个候选节点打分并过滤
//
// 处理流程：
// 1. 获取调度策略：从 Pod 注解或全局配置获取节点调度策略
// 2. 并发处理节点：为每个节点启动一个 goroutine 进行评分
// 3. 设备匹配：调用 fitInDevices 检查节点是否满足资源需求
// 4. 计算分数：
//   - ComputeDefaultScore: 基于当前使用率的基础分数
//   - OverrideScore: 考虑新分配后的最终分数
//
// 5. 收集结果：将满足条件的节点加入结果列表
//
// 为什么使用并发处理？
// - 节点数量可能很多（几百上千个）
// - 每个节点的计算相互独立
// - 并发处理大幅提升性能（从秒级降到毫秒级）
//
// 为什么需要两次打分？
//   - ComputeDefaultScore: 计算节点当前的资源使用情况
//     公式：score = weight × (used/total + usedCore/totalCore + usedMem/totalMem)
//   - OverrideScore: 模拟分配后的情况，计算最终分数
//     考虑：NUMA 亲和性、设备类型、自定义策略
//
// 快照（Snapshot）的作用：
// - 保存设备分配前的状态
// - OverrideScore 需要对比分配前后的变化
// - 用于计算分配的"影响分数"
//
// 失败原因聚合：
// - 按失败类型分组（资源不足、不健康等）
// - 记录每种失败类型影响的节点列表
// - 生成详细的事件消息，帮助用户排查问题
//
// 为什么只在所有节点都失败时记录事件？
// - 成功调度时不需要记录失败原因
// - 避免产生大量无用的事件
// - 失败时提供完整的诊断信息
//
// 参数：
// - nodes: 候选节点的设备使用情况
// - resourceReqs: Pod 的资源请求（按容器分组）
// - task: 待调度的 Pod
// - failedNodes: 已失败的节点（会被更新）
//
// 返回值：
// - *policy.NodeScoreList: 满足条件的节点及其分数
// - error: 处理过程中的错误（聚合所有 goroutine 的错误）
func (s *Scheduler) calcScore(nodes *map[string]*NodeUsage, resourceReqs device.PodDeviceRequests, task *corev1.Pod, failedNodes map[string]string) (*policy.NodeScoreList, error) {
	userNodePolicy := config.NodeSchedulerPolicy
	if task.GetAnnotations() != nil {
		if value, ok := task.GetAnnotations()[util.NodeSchedulerPolicyAnnotationKey]; ok {
			userNodePolicy = value
		}
	}
	res := policy.NodeScoreList{
		Policy:   userNodePolicy,
		NodeList: make([]*policy.NodeScore, 0),
	}

	wg := sync.WaitGroup{}
	fitNodesMutex := sync.Mutex{}
	failedNodesMutex := sync.Mutex{}
	failureReason := make(map[string][]string)
	errCh := make(chan error, len(*nodes))
	for nodeID, node := range *nodes {
		wg.Add(1)
		go func(nodeID string, node *NodeUsage) {
			defer wg.Done()

			viewStatus(*node)
			score := policy.NodeScore{NodeID: nodeID, Node: node.Node, Devices: make(device.PodDevices), Score: 0}
			score.ComputeDefaultScore(node.Devices)
			snapshot := score.SnapshotDevice(node.Devices)

			nodeInfo, err := s.GetNode(nodeID)
			if err != nil {
				klog.ErrorS(err, "Failed to get node", "nodeID", nodeID)
				errCh <- err
				return
			}

			// Assume the node is a fit by default. This handles pods with no device
			// requests, which should be schedulable on any node.
			ctrfit := true
			deviceType := ""
			//This loop is for different container request
			for ctrid, n := range resourceReqs {
				sums := 0
				for _, k := range n {
					sums += int(k.Nums)
				}

				// container need no device and we have got certain deviceType
				if sums == 0 && deviceType != "" {
					score.Devices[deviceType] = append(score.Devices[deviceType], device.ContainerDevices{})
					continue
				}
				klog.V(5).InfoS("fitInDevices", "pod", klog.KObj(task), "node", nodeID)
				fit, reason := fitInDevices(node, n, task, nodeInfo, &score.Devices)
				// found certain deviceType, fill missing empty allocation for containers before this
				for idx := range score.Devices {
					deviceType = idx
					for len(score.Devices[idx]) <= ctrid {
						emptyContainerDevices := device.ContainerDevices{}
						emptyPodSingleDevice := device.PodSingleDevice{}
						emptyPodSingleDevice = append(emptyPodSingleDevice, emptyContainerDevices)
						score.Devices[idx] = append(emptyPodSingleDevice, score.Devices[idx]...)
					}
				}
				ctrfit = fit
				if !fit {
					klog.V(4).InfoS(common.NodeUnfitPod, "pod", klog.KObj(task), "node", nodeID, "reason", reason)
					failedNodesMutex.Lock()
					failedNodes[nodeID] = common.NodeUnfitPod
					for reasonType := range common.ParseReason(reason) {
						failureReason[reasonType] = append(failureReason[reasonType], nodeID)
					}
					failedNodesMutex.Unlock()
					break
				}
			}

			if ctrfit {
				fitNodesMutex.Lock()
				res.NodeList = append(res.NodeList, &score)
				fitNodesMutex.Unlock()
				score.OverrideScore(snapshot, userNodePolicy)
				klog.V(4).InfoS(common.NodeFitPod, "pod", klog.KObj(task), "node", nodeID, "score", score.Score)
			}
		}(nodeID, node)
	}
	wg.Wait()
	close(errCh)

	// only pod scheduler failure will record failure event
	if len(res.NodeList) == 0 {
		for reasonType, failureNodes := range failureReason {
			sort.Strings(failureNodes)
			reason := fmt.Errorf("%d nodes %s(%s)", len(failureNodes), reasonType, strings.Join(failureNodes, ","))
			s.recordScheduleFilterResultEvent(task, EventReasonFilteringFailed, "", reason)
		}
	}

	var errorsSlice []error
	for e := range errCh {
		errorsSlice = append(errorsSlice, e)
	}
	return &res, utilerrors.NewAggregate(errorsSlice)
}
