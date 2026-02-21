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

	"k8s.io/klog/v2"
)

// DeviceListsScore 设备列表的分数结构
// 用于设备级别的调度决策
//
// 字段说明：
// - Device: 设备的使用情况（ID、已用/总量等）
// - Score: 设备的匹配分数（用于排序）
type DeviceListsScore struct {
	Device *device.DeviceUsage
	// Score recode every device user/allocate score
	Score float32
}

// DeviceUsageList 设备使用列表
// 实现了 sort.Interface，支持按策略排序
//
// 字段说明：
// - DeviceLists: 设备列表
// - Policy: 调度策略（binpack 或 spread）
type DeviceUsageList struct {
	DeviceLists []*DeviceListsScore
	Policy      string
}

// Len 返回设备列表长度
// sort.Interface 的必需方法
func (l DeviceUsageList) Len() int {
	return len(l.DeviceLists)
}

// Swap 交换两个设备的位置
// sort.Interface 的必需方法
func (l DeviceUsageList) Swap(i, j int) {
	l.DeviceLists[i], l.DeviceLists[j] = l.DeviceLists[j], l.DeviceLists[i]
}

// Less 定义设备的排序规则
// 这是 GPU 级别调度策略的实现
//
// Binpack 策略（装箱）：
// - 优先使用已分配的设备（分数低的排前面）
// - 同 NUMA 节点优先级更高（Numa 大的排前面）
// - 目标：减少设备碎片，提高利用率
//
// Spread 策略（分散）- 默认：
// - 优先使用空闲设备（分数高的排前面）
// - 同 NUMA 节点优先级更低（Numa 小的排前面）
// - 目标：均衡负载，提高并发性能
//
// NUMA 的作用：
// - NUMA（Non-Uniform Memory Access）影响设备间通信性能
// - 同 NUMA 节点的设备通信延迟更低
// - Binpack 倾向于同 NUMA（提高单任务性能）
// - Spread 倾向于跨 NUMA（分散负载）
//
// 为什么 Binpack 是 Score 小的排前面？
// - Score 高 = 使用率高
// - 排序后从前往后选择
// - Binpack 要选使用率高的，所以 Less 返回 Score 小
// - 实际排序后，Score 大的在后面，会被优先选择
//
// sort.Interface 的必需方法
func (l DeviceUsageList) Less(i, j int) bool {
	if l.Policy == util.GPUSchedulerPolicyBinpack.String() {
		if l.DeviceLists[i].Device.Numa == l.DeviceLists[j].Device.Numa {
			return l.DeviceLists[i].Score < l.DeviceLists[j].Score
		}
		return l.DeviceLists[i].Device.Numa > l.DeviceLists[j].Device.Numa
	}
	// default policy is spread
	if l.DeviceLists[i].Device.Numa == l.DeviceLists[j].Device.Numa {
		return l.DeviceLists[i].Score > l.DeviceLists[j].Score
	}
	return l.DeviceLists[i].Device.Numa < l.DeviceLists[j].Device.Numa
}

// +scheduler:2.2.2
// ComputeScore 计算设备的匹配分数
// 这是设备级别调度策略的核心
//
// 计算公式：
// score = weight × (usedScore + coreScore + memScore)
// 其中：
// - usedScore = (request + used) / count        // 设备数量维度
// - coreScore = (requestCore + usedCore) / totalCore  // 算力维度
// - memScore = (requestMem + usedMem) / totalMem      // 显存维度
//
// 为什么要三个维度？
// - 设备数量：反映设备的整体使用情况
// - 算力：反映计算资源的使用情况（GPU Core 利用率）
// - 显存：反映内存资源的使用情况（最常见的瓶颈）
// - 综合考虑避免单一维度的偏差
//
// 为什么要加上请求量？
// - 模拟分配后的状态
// - 预测分配对设备使用率的影响
// - 用于排序时选择最合适的设备
//
// 分数的含义：
// - Binpack 策略：分数越高越好（优先使用已分配的设备）
// - Spread 策略：分数越低越好（优先使用空闲设备）
// - 排序时通过 Less 方法实现不同策略
//
// 百分比内存的处理：
// - MemPercentagereq = 0 或 101：使用绝对值 Memreq
// - 其他值：按百分比计算 = Totalmem × (MemPercentagereq / 100)
// - 支持两种内存请求方式，提高灵活性
//
// 参数：
// - requests: 容器的设备请求列表
//
// 副作用：
// - 更新 ds.Score 字段
func (ds *DeviceListsScore) ComputeScore(requests device.ContainerDeviceRequests) {
	request, core, mem := int32(0), int32(0), int32(0)
	// Here we are required to use the same type device
	for _, container := range requests {
		request += container.Nums
		core += container.Coresreq
		if container.MemPercentagereq != 0 && container.MemPercentagereq != 101 {
			mem += ds.Device.Totalmem * (container.MemPercentagereq / 100.0)
			continue
		}
		mem += container.Memreq
	}
	klog.V(2).Infof("device %s user %d, userCore %d, userMem %d,", ds.Device.ID, ds.Device.Used, ds.Device.Usedcores, ds.Device.Usedmem)

	usedScore := float32(request+ds.Device.Used) / float32(ds.Device.Count)
	coreScore := float32(core+ds.Device.Usedcores) / float32(ds.Device.Totalcore)
	memScore := float32(mem+ds.Device.Usedmem) / float32(ds.Device.Totalmem)
	ds.Score = float32(util.Weight) * (usedScore + coreScore + memScore)
	klog.V(2).Infof("device %s computer score is %f", ds.Device.ID, ds.Score)
}
