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
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	coordinationv1 "k8s.io/client-go/listers/coordination/v1"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/config"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/policy"
	"github.com/Project-HAMi/HAMi/pkg/util"
	"github.com/Project-HAMi/HAMi/pkg/util/client"
	"github.com/Project-HAMi/HAMi/pkg/util/leaderelection"
	nodelockutil "github.com/Project-HAMi/HAMi/pkg/util/nodelock"
)

const (
	defaultResync    = 1 * time.Hour
	syncedPollPeriod = 100 * time.Millisecond
)

type Scheduler struct {
	*nodeManager
	podManager    *device.PodManager
	quotaManager  *device.QuotaManager
	leaderManager leaderelection.LeaderManager

	stopCh       chan struct{}
	nodeNotify   chan struct{}
	leaderNotify chan struct{}

	kubeClient  kubernetes.Interface
	podLister   listerscorev1.PodLister
	nodeLister  listerscorev1.NodeLister
	quotaLister listerscorev1.ResourceQuotaLister
	leaseLister coordinationv1.LeaseLister
	//Node status returned by filter
	cachedstatus map[string]*NodeUsage
	//Node Overview
	overviewstatus map[string]*NodeUsage
	eventRecorder  record.EventRecorder
	started        uint32 // 0 = false, 1 = true

	lock   sync.RWMutex
	synced bool
}

// +scheduler:0
// NewScheduler 创建并初始化调度器实例
// 这是整个调度系统的核心入口
//
// 初始化内容：
// 1. 创建三大管理器：NodeManager（节点管理）、PodManager（Pod管理）、QuotaManager（配额管理）
// 2. 初始化通知通道：nodeNotify（节点变化）、leaderNotify（选举通知）
// 3. 配置 Leader 选举：支持多副本高可用部署
// 4. 初始化状态缓存：cachedstatus（过滤结果）、overviewstatus（全局视图）
//
// 为什么需要三个管理器？
// - NodeManager: 维护集群中所有节点的设备信息（从节点注解读取）
// - PodManager: 跟踪 Pod 的设备分配记录（用于计算设备使用情况）
// - QuotaManager: 管理命名空间级别的资源配额（防止资源超分）
//
// 为什么需要 Leader 选举？
// - 支持多副本部署，提高可用性
// - 只有 Leader 处理调度请求，避免冲突
// - 自动故障转移，Leader 失败时自动选举新 Leader
//
// 为什么有两个通知通道？
// - nodeNotify: 节点增删时触发，重新扫描节点设备
// - leaderNotify: 成为 Leader 时触发，立即同步状态
//
// 返回值：
// - *Scheduler: 初始化完成的调度器实例
func NewScheduler() *Scheduler {
	klog.InfoS("Initializing HAMi scheduler")
	s := &Scheduler{
		stopCh:       make(chan struct{}),
		cachedstatus: make(map[string]*NodeUsage),
		nodeNotify:   make(chan struct{}, 1),
		leaderNotify: make(chan struct{}, 1),
		started:      0,
		synced:       false,
	}
	s.nodeManager = newNodeManager()
	s.podManager = device.NewPodManager()
	s.quotaManager = device.NewQuotaManager()
	// Use dummy leader manager when leaderElect is disabled
	// This ensures IsLeader() always returns true and synced will not be set to false
	s.leaderManager = leaderelection.NewDummyLeaderManager(true)
	if config.LeaderElect {
		callbacks := leaderelection.LeaderCallbacks{
			OnStartedLeading: func() {
				s.leaderNotify <- struct{}{}
			},
			OnStoppedLeading: func() {
				s.lock.Lock()
				defer s.lock.Unlock()
				s.synced = false
			},
		}
		s.leaderManager = leaderelection.NewLeaderManager(config.HostName, config.LeaderElectResourceNamespace, config.LeaderElectResourceName, callbacks)
	}
	klog.V(2).InfoS("Scheduler initialized successfully")
	return s
}

func (s *Scheduler) GetQuotaManager() *device.QuotaManager {
	return s.quotaManager
}

func (s *Scheduler) GetPodManager() *device.PodManager {
	return s.podManager
}

func (s *Scheduler) GetLeaderManager() leaderelection.LeaderManager {
	return s.leaderManager
}

func (s *Scheduler) doNodeNotify() {
	select {
	case s.nodeNotify <- struct{}{}:
	default:
	}
}

func (s *Scheduler) onAddPod(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		klog.ErrorS(fmt.Errorf("invalid pod object"), "Failed to process pod addition")
		return
	}
	klog.V(5).InfoS("Pod added", "pod", pod.Name, "namespace", pod.Namespace)
	nodeID, ok := pod.Annotations[util.AssignedNodeAnnotations]
	if !ok {
		return
	}
	if util.IsPodInTerminatedState(pod) {
		pi, ok := s.podManager.GetPod(pod)
		if ok {
			s.quotaManager.RmUsage(pod, pi.Devices)
		}
		s.podManager.DelPod(pod)
		return
	}
	podDev, _ := device.DecodePodDevices(device.SupportDevices, pod.Annotations)
	if s.podManager.AddPod(pod, nodeID, podDev) {
		s.quotaManager.AddUsage(pod, podDev)
	}
}

func (s *Scheduler) onUpdatePod(_, newObj any) {
	s.onAddPod(newObj)
}

func (s *Scheduler) onDelPod(obj any) {
	var pod *corev1.Pod
	var ok bool

	switch t := obj.(type) {
	case *corev1.Pod:
		pod = t
		klog.V(4).InfoS("Pod deleted, cleaning up cache", "pod", pod.Namespace+"/"+pod.Name)
	case cache.DeletedFinalStateUnknown:
		if pod, ok = t.Obj.(*corev1.Pod); ok {
			klog.V(4).InfoS("Pod tombstone deleted, cleaning up cache", "pod", t.Key)
		} else {
			klog.Errorf("Received tombstone for non-pod object on pod delete")
		}
	default:
		klog.Errorf("Received unknown object type on pod delete")
		return
	}

	_, ok = pod.Annotations[util.AssignedNodeAnnotations]
	if !ok {
		return
	}
	pi, ok := s.podManager.GetPod(pod)
	if ok {
		s.quotaManager.RmUsage(pod, pi.Devices)
		s.podManager.DelPod(pod)
	}
}

// onDelNode handles node delete events. It removes any in-memory per-node
// lock bookkeeping to avoid unbounded growth when nodes are removed by
// autoscalers or administratively.
func (s *Scheduler) onDelNode(obj any) {
	// Ensure downstream consumers are notified regardless of decoding success
	defer s.doNodeNotify()

	var nodeName string
	switch t := obj.(type) {
	case *corev1.Node:
		nodeName = t.Name
		klog.V(4).InfoS("Node deleted, cleaning up nodelock", "node", nodeName)
	case cache.DeletedFinalStateUnknown:
		if n, ok := t.Obj.(*corev1.Node); ok {
			nodeName = n.Name
			klog.V(4).InfoS("Node tombstone deleted, cleaning up nodelock", "node", nodeName)
		} else {
			klog.V(5).InfoS("Received tombstone for non-node object on delete")
			return
		}
	default:
		klog.V(5).InfoS("Received unknown object type on node delete")
		return
	}

	nodelockutil.CleanupNodeLock(nodeName)
	s.rmNode(nodeName)
	s.cleanupNodeUsage(nodeName)
}

// cleanupNodeUsage removes the node from overviewstatus and cachedstatus maps
// to ensure metrics no longer report data for deleted nodes.
func (s *Scheduler) cleanupNodeUsage(nodeID string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.overviewstatus[nodeID]; ok {
		delete(s.overviewstatus, nodeID)
		klog.V(4).InfoS("Removed node from overviewstatus", "node", nodeID)
	}
	if _, ok := s.cachedstatus[nodeID]; ok {
		delete(s.cachedstatus, nodeID)
		klog.V(4).InfoS("Removed node from cachedstatus", "node", nodeID)
	}
}

func (s *Scheduler) onAddQuota(obj any) {
	quota, ok := obj.(*corev1.ResourceQuota)
	if !ok {
		klog.Errorf("unknown add object type")
		return
	}
	s.quotaManager.AddQuota(quota)
}

func (s *Scheduler) onUpdateQuota(oldObj, newObj any) {
	s.onDelQuota(oldObj)
	s.onAddQuota(newObj)
}

func (s *Scheduler) onDelQuota(obj any) {
	quota, ok := obj.(*corev1.ResourceQuota)
	if !ok {
		klog.Errorf("unknown del object type")
		return
	}
	s.quotaManager.DelQuota(quota)
}

func (s *Scheduler) Start() error {
	klog.InfoS("Starting HAMi scheduler components")
	s.kubeClient = client.GetClient()
	informerFactory := informers.NewSharedInformerFactoryWithOptions(s.kubeClient, defaultResync)
	s.podLister = informerFactory.Core().V1().Pods().Lister()
	s.nodeLister = informerFactory.Core().V1().Nodes().Lister()
	s.quotaLister = informerFactory.Core().V1().ResourceQuotas().Lister()

	podEventHandlerRegistration, err := informerFactory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    s.onAddPod,
		UpdateFunc: s.onUpdatePod,
		DeleteFunc: s.onDelPod,
	})
	if err != nil {
		return fmt.Errorf("failed to register pod event handler: %v", err)
	}
	nodeEventHandlerRegistration, err := informerFactory.Core().V1().Nodes().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ any) { s.doNodeNotify() },
		DeleteFunc: s.onDelNode,
	})
	if err != nil {
		return fmt.Errorf("failed to register node event handler: %v", err)
	}
	resourceQuotaEventHandlerRegistration, err := informerFactory.Core().V1().ResourceQuotas().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    s.onAddQuota,
		UpdateFunc: s.onUpdateQuota,
		DeleteFunc: s.onDelQuota,
	})
	if err != nil {
		return fmt.Errorf("failed to register resource quota event handler: %v", err)
	}

	informerFactory.Start(s.stopCh)
	informerFactory.WaitForCacheSync(s.stopCh)
	cache.WaitForCacheSync(s.stopCh, podEventHandlerRegistration.HasSynced, nodeEventHandlerRegistration.HasSynced, resourceQuotaEventHandlerRegistration.HasSynced)

	if config.LeaderElect {
		leaseInformerFactory := informers.NewSharedInformerFactoryWithOptions(s.kubeClient, defaultResync, informers.WithNamespace(config.LeaderElectResourceNamespace))
		s.leaseLister = leaseInformerFactory.Coordination().V1().Leases().Lister()

		leaseEventHandlerRegistration, err := leaseInformerFactory.Coordination().V1().Leases().Informer().AddEventHandler(s.leaderManager)
		if err != nil {
			return fmt.Errorf("failed to register lease event handler: %w", err)
		}
		leaseInformerFactory.Start(s.stopCh)
		leaseInformerFactory.WaitForCacheSync(s.stopCh)
		cache.WaitForCacheSync(s.stopCh, leaseEventHandlerRegistration.HasSynced)
	}

	s.addAllEventHandlers()
	atomic.StoreUint32(&s.started, 1)
	return nil
}

func (s *Scheduler) Stop() {
	close(s.stopCh)
}

// +scheduler:0.1
// RegisterFromNodeAnnotations 持续监听并注册节点设备信息
// 这是调度器的后台任务，负责维护节点设备的最新状态
//
// 工作流程：
// 1. 监听三种触发事件：
//   - nodeNotify: 节点增删事件（来自 Informer）
//   - leaderNotify: 成为 Leader 事件（来自选举）
//   - ticker: 定时触发（每 15 秒）
//
// 2. 调用 register 方法扫描所有节点
// 3. 从节点注解中解析设备信息
// 4. 更新 NodeManager 中的设备列表
//
// 为什么需要三种触发方式？
// - nodeNotify: 及时响应节点变化，保证数据实时性
// - leaderNotify: 新 Leader 立即同步状态，避免数据不一致
// - ticker: 兜底机制，防止事件丢失，定期全量同步
//
// 为什么使用 select？
// - 非阻塞的多路复用，任何一个事件到达都会触发处理
// - stopCh 用于优雅退出，避免 goroutine 泄漏
// - 通道缓冲区为 1，避免事件堆积（只需要知道"有变化"即可）
//
// 为什么检查 started 标志？
// - Informer 缓存可能还未同步完成
// - 避免基于不完整数据进行注册
// - 等待 Start() 方法完成初始化
//
// printedLog 的作用：
// - 记录已打印过日志的节点
// - 避免重复打印相同的节点信息（减少日志噪音）
// - 只在首次发现节点时打印 Info 级别日志
//
// 这个方法应该在 goroutine 中运行：
//
//	go scheduler.RegisterFromNodeAnnotations()
func (s *Scheduler) RegisterFromNodeAnnotations() {
	klog.InfoS("Entering RegisterFromNodeAnnotations")
	defer klog.InfoS("Exiting RegisterFromNodeAnnotations")

	labelSelector := labels.Set(config.NodeLabelSelector).AsSelector()
	klog.InfoS("Using label selector for list nodes", "selector", labelSelector.String())

	ticker := time.NewTicker(time.Second * 15)
	defer ticker.Stop()
	printedLog := map[string]bool{}
	for {
		select {
		case <-s.nodeNotify:
			klog.V(5).InfoS("Received node notification")
		case <-s.leaderNotify:
			klog.V(5).InfoS("Received leaderElection notification. We are just elected to leader")
		case <-ticker.C:
			klog.V(5).InfoS("Ticker triggered")
		case <-s.stopCh:
			klog.InfoS("Received stop signal, exiting RegisterFromNodeAnnotations")
			return
		}
		if atomic.LoadUint32(&s.started) == 0 {
			klog.V(5).InfoS("Scheduler not started yet, skipping ...")
			continue
		}
		s.register(labelSelector, printedLog)
	}
}

func (s *Scheduler) register(labelSelector labels.Selector, printedLog map[string]bool) {
	// Lock here to avoid setting s.synced to false, when we lost leadership, while doing register.
	// 1. lost leadership before register: synced will set to false in callbacks, and register will be skipped because IsLeader() returns false
	// 2. lost leadership during or after register: synced will set to true after finishing register, and callback will set it to false again after lock is acquired by callback
	s.lock.Lock()
	defer s.lock.Unlock()
	// Only do registration when we are leader
	if !s.leaderManager.IsLeader() {
		klog.V(5).InfoS("Scheduler is not leader yet, skipping ...")
		return
	}
	rawNodes, err := s.nodeLister.List(labelSelector)
	if err != nil {
		klog.ErrorS(err, "Failed to list nodes with selector", "selector", labelSelector.String())
		return
	}
	klog.V(5).InfoS("Listed nodes", "nodeCount", len(rawNodes))
	var nodeNames []string
	for _, val := range rawNodes {
		nodeNames = append(nodeNames, val.Name)
		klog.V(5).InfoS("Processing node", "nodeName", val.Name)

		for devhandsk, devInstance := range device.GetDevices() {
			klog.V(5).InfoS("Checking device health", "nodeName", val.Name, "deviceVendor", devhandsk)

			nodedevices, err := devInstance.GetNodeDevices(*val)
			if err != nil {
				klog.V(5).InfoS("Failed to get node devices", "nodeName", val.Name, "deviceVendor", devhandsk)
				continue
			}

			health, needUpdate := devInstance.CheckHealth(devhandsk, val)
			klog.V(5).InfoS("Device health check result", "nodeName", val.Name, "deviceVendor", devhandsk, "health", health, "needUpdate", needUpdate)

			if !health {
				klog.Warning("Device is unhealthy, cleaning up node", "nodeName", val.Name, "deviceVendor", devhandsk)
				err := devInstance.NodeCleanUp(val.Name)
				if err != nil {
					klog.ErrorS(err, "Node cleanup failed", "nodeName", val.Name, "deviceVendor", devhandsk)
				}

				// 从缓存中，删除节点上对应的设备
				s.rmNodeDevices(val.Name, devhandsk)
				continue
			}
			if !needUpdate {
				klog.V(5).InfoS("No update needed for device", "nodeName", val.Name, "deviceVendor", devhandsk)
				continue
			}
			nodeInfo := &device.NodeInfo{}
			nodeInfo.ID = val.Name
			nodeInfo.Node = val
			klog.V(5).InfoS("Fetching node devices", "nodeName", val.Name, "deviceVendor", devhandsk)
			nodeInfo.Devices = make(map[string][]device.DeviceInfo, 0)
			for _, deviceinfo := range nodedevices {
				nodeInfo.Devices[deviceinfo.DeviceVendor] = append(nodeInfo.Devices[deviceinfo.DeviceVendor], *deviceinfo)
			}
			s.addNode(val.Name, nodeInfo)
			if s.nodes[val.Name] != nil && len(nodeInfo.Devices) > 0 {
				if printedLog[val.Name] {
					klog.V(5).InfoS("Node device updated", "nodeName", val.Name, "deviceVendor", devhandsk, "nodeInfo", nodeInfo, "totalDevices", s.nodes[val.Name].Devices)
				} else {
					klog.InfoS("Node device added", "nodeName", val.Name, "deviceVendor", devhandsk, "nodeInfo", nodeInfo, "totalDevices", s.nodes[val.Name].Devices)
					printedLog[val.Name] = true
				}
			}
		}
	}
	_, _, err = s.getNodesUsage(&nodeNames, nil)
	if err != nil {
		klog.ErrorS(err, "Failed to get node usage", "nodeNames", nodeNames)
		return
	}

	// Set synced to true only after getNodeUsage() succeeds
	s.synced = true
}

// +scheduler:2.0
// WaitForCacheSync 等待调度器缓存同步完成
// 这是 Filter 流程的前置检查，确保数据完整性
//
// 同步条件：
// 1. Informer 缓存已同步（HasSynced）
// 2. 至少成功执行一次 register（synced = true）
// 3. 当前实例是 Leader（如果启用选举）
//
// 为什么需要等待同步？
// - Informer 启动时需要时间从 API Server 拉取数据
// - 基于不完整数据调度可能导致设备重复分配
// - 确保调度决策的正确性和一致性
//
// 轮询机制：
// - 每 100ms 检查一次 synced 标志
// - 使用 context 控制超时（由调用方设置）
// - 支持优雅取消（context.Cancel）
//
// 何时会返回 false？
// - context 被取消（超时或主动取消）
// - 调度器停止（stopCh 关闭）
// - 失去 Leader 身份（synced 被设为 false）
//
// 调用时机：
// - 每次 Filter 请求开始时调用
// - Readiness 探针检查时调用
// - 确保只有同步完成的实例才处理请求
//
// 参数：
// - ctx: 上下文，用于超时控制
//
// 返回值：
// - bool: true 表示同步完成，false 表示超时或取消
func (s *Scheduler) WaitForCacheSync(ctx context.Context) bool {
	err := wait.PollUntilContextCancel(ctx, syncedPollPeriod, true, func(context.Context) (done bool, err error) {
		s.lock.RLock()
		defer s.lock.RUnlock()
		return s.synced, nil
	})
	if err != nil {
		klog.ErrorS(err, "failed to poll until context cancel")
		return false
	}

	return true
}

// InspectAllNodesUsage is used by metrics monitor.
func (s *Scheduler) InspectAllNodesUsage() *map[string]*NodeUsage {
	return &s.overviewstatus
}

// returns all nodes and its device memory usage, and we filter it with nodeSelector, taints, nodeAffinity
// unschedulerable and nodeName.
// +scheduler:2.1
// getNodesUsage 获取节点的设备使用情况
// 这是 Filter 流程的第一步：构建节点设备的完整视图
//
// 处理流程：
// 1. 初始化节点设备列表：从 NodeManager 获取所有节点的设备信息
// 2. 创建设备使用对象：为每个设备创建 DeviceUsage 结构，初始使用量为 0
// 3. 累加已分配资源：遍历 PodManager 中的所有 Pod，累加设备使用量
// 4. 处理 MIG 设备：特殊处理 NVIDIA MIG（Multi-Instance GPU）的使用情况
// 5. 过滤候选节点：只返回参数中指定的候选节点
//
// 为什么要区分 overallnodeMap 和 cachenodeMap？
// - overallnodeMap: 包含所有节点，用于监控指标和全局视图
// - cachenodeMap: 只包含候选节点，用于本次调度决策
// - 分离设计减少不必要的计算，提高性能
//
// 为什么要遍历所有 Pod？
// - 设备使用情况是动态的，需要实时计算
// - PodManager 维护了所有已分配设备的 Pod 列表
// - 通过累加得到每个设备的当前使用量
//
// MIG 设备的特殊处理：
// - MIG 允许将一个物理 GPU 分割成多个独立实例
// - UUID 格式：GPU-xxx[template:instance]，例如 GPU-123[1g.5gb:0]
// - 需要解析 UUID，标记对应的 MIG 实例为已使用
// - 检查模式冲突：MIG 模式和 hami-core 模式不能混用
//
// 参数：
// - nodes: 候选节点列表（来自 Kubernetes 调度器）
// - task: 待调度的 Pod（用于获取调度策略）
//
// 返回值：
// - *map[string]*NodeUsage: 候选节点的设备使用情况
// - map[string]string: 失败节点及原因
// - error: 处理过程中的错误
func (s *Scheduler) getNodesUsage(nodes *[]string, task *corev1.Pod) (*map[string]*NodeUsage, map[string]string, error) {
	overallnodeMap := make(map[string]*NodeUsage)
	cachenodeMap := make(map[string]*NodeUsage)
	failedNodes := make(map[string]string)
	allNodes, err := s.ListNodes()
	if err != nil {
		return &overallnodeMap, failedNodes, err
	}

	// 遍历所有 node 获取资源使用情况
	for _, node := range allNodes {
		nodeInfo := &NodeUsage{}
		userGPUPolicy := util.GetGPUSchedulerPolicyByPod(device.GPUSchedulerPolicy, task)
		nodeInfo.Node = node.Node
		nodeInfo.Devices = policy.DeviceUsageList{
			Policy:      userGPUPolicy,
			DeviceLists: make([]*policy.DeviceListsScore, 0),
		}
		for _, k := range node.Devices {
			for _, d := range k {
				nodeInfo.Devices.DeviceLists = append(nodeInfo.Devices.DeviceLists, &policy.DeviceListsScore{
					Score: 0,
					Device: &device.DeviceUsage{
						ID:        d.ID,
						Index:     d.Index,
						Used:      0,
						Count:     d.Count,
						Usedmem:   0,
						Totalmem:  d.Devmem,
						Totalcore: d.Devcore,
						Usedcores: 0,
						MigUsage: device.MigInUse{
							Index:     0,
							UsageList: make(device.MIGS, 0),
						},
						MigTemplate: d.MIGTemplate,
						Mode:        d.Mode,
						Type:        d.Type,
						Numa:        d.Numa,
						Health:      d.Health,
						PodInfos:    make([]*device.PodInfo, 0),
						CustomInfo:  maps.Clone(d.CustomInfo),
					},
				})
			}
		}
		overallnodeMap[node.ID] = nodeInfo
	}

	podsInfo := s.podManager.ListPodsInfo()
	for _, p := range podsInfo {
		node, ok := overallnodeMap[p.NodeID]
		if !ok {
			klog.V(5).InfoS("pod allocated unknown node resources",
				"pod", klog.KRef(p.Namespace, p.Name), "nodeID", p.NodeID)
			continue
		}
		for _, podsingleds := range p.Devices {
			for _, ctrdevs := range podsingleds {
				for _, udevice := range ctrdevs {
					for _, d := range node.Devices.DeviceLists {
						deviceID := udevice.UUID
						if strings.Contains(deviceID, "[") {
							deviceID = strings.Split(deviceID, "[")[0]
						}
						if d.Device.ID == deviceID {
							d.Device.Used++
							d.Device.Usedmem += udevice.Usedmem
							d.Device.Usedcores += udevice.Usedcores
							d.Device.PodInfos = append(d.Device.PodInfos, p)

							if strings.Contains(udevice.UUID, "[") {
								if strings.Compare(d.Device.Mode, "hami-core") == 0 {
									klog.Errorf("found a mig task running on a hami-core GPU\n")
									d.Device.Health = false
									continue
								}
								tmpIdx, Instance, _ := device.ExtractMigTemplatesFromUUID(udevice.UUID)
								if len(d.Device.MigUsage.UsageList) == 0 {
									device.PlatternMIG(&d.Device.MigUsage, d.Device.MigTemplate, tmpIdx)
								}
								d.Device.MigUsage.UsageList[Instance].InUse = true
								klog.V(5).Infoln("add mig usage", d.Device.MigUsage, "template=", d.Device.MigTemplate, "uuid=", d.Device.ID)
							}
						}
					}
				}
			}
		}
		klog.V(5).Infof("usage: pod %v assigned %v %v", p.Name, p.NodeID, p.Devices)
	}
	s.overviewstatus = overallnodeMap
	for _, nodeID := range *nodes {
		node, err := s.GetNode(nodeID)
		if err != nil {
			// The identified node does not have a gpu device, so the log here has no practical meaning,increase log priority.
			klog.V(5).InfoS("node unregistered", "node", nodeID, "error", err)
			failedNodes[nodeID] = "node unregistered"
			continue
		}
		cachenodeMap[node.ID] = overallnodeMap[node.ID]
	}
	s.cachedstatus = cachenodeMap
	return &cachenodeMap, failedNodes, nil
}

func (s *Scheduler) getPodUsage() (map[string]device.PodUseDeviceStat, error) {
	podUsageStat := make(map[string]device.PodUseDeviceStat)
	pods, err := s.podLister.List(labels.NewSelector())
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodSucceeded {
			continue
		}
		podUseDeviceNum := 0
		if v, ok := pod.Annotations[util.DeviceBindPhase]; ok && v == util.DeviceBindSuccess {
			podUseDeviceNum = 1
		}
		nodeName := pod.Spec.NodeName
		if _, ok := podUsageStat[nodeName]; !ok {
			podUsageStat[nodeName] = device.PodUseDeviceStat{
				TotalPod:     1,
				UseDevicePod: podUseDeviceNum,
			}
		} else {
			exist := podUsageStat[nodeName]
			podUsageStat[nodeName] = device.PodUseDeviceStat{
				TotalPod:     exist.TotalPod + 1,
				UseDevicePod: exist.UseDevicePod + podUseDeviceNum,
			}
		}
	}
	return podUsageStat, nil
}

// +scheduler:3
// Bind 处理 Kubernetes 调度器的绑定请求
// 这是调度流程的第三步：将 Pod 绑定到选定的节点
//
// 处理流程：
// 1. 获取 Pod 和 Node 对象：从 API Server 获取最新状态
// 2. 锁定节点：为所有设备类型获取节点锁，防止并发冲突
// 3. 更新 Pod 状态：标记为 "allocating"，记录绑定时间
// 4. 执行绑定：调用 Kubernetes API 将 Pod 绑定到节点
// 5. 释放锁：成功或失败都要释放节点锁
//
// 为什么需要节点锁？
// - 多个 Pod 可能同时调度到同一节点
// - 锁机制防止设备被重复分配
// - 使用 Kubernetes Lease 对象实现分布式锁
//
// 为什么要分两步（Filter + Bind）？
// - Filter 阶段只是"预分配"，不修改实际状态
// - Bind 阶段才真正执行绑定，此时需要独占访问
// - 分离设计提高并发性能（Filter 可以并行）
//
// 为什么使用 goto？
// - 统一的错误处理路径，确保锁一定被释放
// - 避免多个 defer 导致的复杂性
// - 清晰的资源清理逻辑
//
// 绑定失败会怎样？
// - 返回错误给 Kubernetes 调度器
// - 调度器会重新调用 Filter，选择其他节点
// - Pod 注解中的分配信息会被清理（通过 PodManager）
//
// 参数：
// - args: 包含 Pod 名称、命名空间、目标节点的绑定参数
//
// 返回值：
// - *extenderv1.ExtenderBindingResult: 包含绑定结果和错误信息
// - error: 处理过程中的错误
func (s *Scheduler) Bind(args extenderv1.ExtenderBindingArgs) (*extenderv1.ExtenderBindingResult, error) {
	klog.InfoS("Attempting to bind pod to node", "pod", args.PodName, "namespace", args.PodNamespace, "node", args.Node)
	var res *extenderv1.ExtenderBindingResult

	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: args.PodName, UID: args.PodUID},
		Target:     corev1.ObjectReference{Kind: "Node", Name: args.Node},
	}
	current, err := s.kubeClient.CoreV1().Pods(args.PodNamespace).Get(context.Background(), args.PodName, metav1.GetOptions{})
	if err != nil {
		klog.ErrorS(err, "Failed to get pod", "pod", args.PodName, "namespace", args.PodNamespace)
		return &extenderv1.ExtenderBindingResult{Error: err.Error()}, err
	}
	klog.InfoS("Trying to get the target node for pod", "pod", args.PodName, "namespace", args.PodNamespace, "node", args.Node)
	node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), args.Node, metav1.GetOptions{})
	if err != nil {
		klog.ErrorS(err, "Failed to get node", "node", args.Node)
		s.recordScheduleBindingResultEvent(current, EventReasonBindingFailed, []string{}, fmt.Errorf("failed to get node %s", args.Node))
		res = &extenderv1.ExtenderBindingResult{Error: err.Error()}
		return res, nil
	}

	tmppatch := map[string]string{
		util.DeviceBindPhase:     "allocating",
		util.BindTimeAnnotations: strconv.FormatInt(time.Now().Unix(), 10),
	}

	for _, val := range device.GetDevices() {
		// 针对当前设备，给节点上锁，其实就是打一个特殊的 anno
		err = val.LockNode(node, current)
		if err != nil {
			klog.ErrorS(err, "Failed to lock node", "node", args.Node, "device", val)
			goto ReleaseNodeLocks
		}
	}

	err = util.PatchPodAnnotations(current, tmppatch)
	if err != nil {
		klog.ErrorS(err, "Failed to patch pod annotations", "pod", klog.KObj(current))
		goto ReleaseNodeLocks
	}

	err = s.kubeClient.CoreV1().Pods(args.PodNamespace).Bind(context.Background(), binding, metav1.CreateOptions{})
	if err != nil {
		klog.ErrorS(err, "Failed to bind pod", "pod", args.PodName, "namespace", args.PodNamespace, "node", args.Node)
		goto ReleaseNodeLocks
	}

	s.recordScheduleBindingResultEvent(current, EventReasonBindingSucceed, []string{args.Node}, nil)
	klog.InfoS("Successfully bound pod to node", "pod", args.PodName, "namespace", args.PodNamespace, "node", args.Node)
	return &extenderv1.ExtenderBindingResult{Error: ""}, nil

ReleaseNodeLocks:
	klog.InfoS("Release node locks", "node", args.Node)
	for _, val := range device.GetDevices() {
		val.ReleaseNodeLock(node, current)
	}
	s.recordScheduleBindingResultEvent(current, EventReasonBindingFailed, []string{}, err)
	return &extenderv1.ExtenderBindingResult{Error: err.Error()}, nil
}

// +scheduler:2
// Filter 处理 Kubernetes 调度器的过滤请求
// 这是调度流程的第二步：从候选节点中选出最适合的节点
//
// 处理流程：
// 1. 解析资源请求：从 Pod 的容器中提取设备资源需求
// 2. 获取节点状态：调用 getNodesUsage 获取所有候选节点的设备使用情况
// 3. 计算节点分数：调用 calcScore 为每个节点打分，过滤不满足条件的节点
// 4. 选择最优节点：按分数排序，选择分数最高的节点
// 5. 分配设备：将选定的设备信息写入 Pod 注解
// 6. 更新状态：在 PodManager 和 QuotaManager 中记录分配结果
//
// 为什么要删除 Pod 再添加？
// - 同一个 Pod 可能多次调用 Filter（调度失败重试）
// - 删除旧记录避免重复计算资源使用
// - 最终的分配结果会在步骤 6 重新添加
//
// 为什么要写入 Pod 注解？
// - 注解是 Kubernetes 传递元数据的标准方式
// - Device Plugin 会读取这些注解，配置容器环境
// - 支持调度器重启后恢复状态
//
// 为什么选择分数最高的节点？
// - 排序后最后一个元素是分数最高的（Less 方法定义）
// - Binpack 策略：分数高 = 已使用多（集中分配）
// - Spread 策略：分数高 = 空闲多（分散分配）
//
// 参数：
// - args: 包含 Pod 对象和候选节点列表的过滤参数
//
// 返回值：
// - *extenderv1.ExtenderFilterResult: 包含选定节点和失败原因的结果
// - error: 处理过程中的错误
func (s *Scheduler) Filter(args extenderv1.ExtenderArgs) (*extenderv1.ExtenderFilterResult, error) {
	klog.InfoS("Starting schedule filter process", "pod", args.Pod.Name, "uuid", args.Pod.UID, "namespace", args.Pod.Namespace)
	resourceReqs := device.Resourcereqs(args.Pod)
	resourceReqTotal := 0
	for _, n := range resourceReqs {
		for _, k := range n {
			resourceReqTotal += int(k.Nums)
		}
	}
	if resourceReqTotal == 0 {
		klog.V(1).InfoS("Pod does not request any resources",
			"pod", args.Pod.Name)
		s.recordScheduleFilterResultEvent(args.Pod, EventReasonFilteringFailed, "", fmt.Errorf("does not request any resource"))
		return &extenderv1.ExtenderFilterResult{
			NodeNames:   args.NodeNames,
			FailedNodes: nil,
			Error:       "",
		}, nil
	}
	// 为啥这里要删除？
	s.podManager.DelPod(args.Pod)

	// 获取节点设备使用信息
	nodeUsage, failedNodes, err := s.getNodesUsage(args.NodeNames, args.Pod)
	if err != nil {
		s.recordScheduleFilterResultEvent(args.Pod, EventReasonFilteringFailed, "", err)
		return nil, err
	}
	if len(failedNodes) != 0 {
		klog.V(5).InfoS("Nodes failed during usage retrieval",
			"nodes", failedNodes)
	}
	nodeScores, err := s.calcScore(nodeUsage, resourceReqs, args.Pod, failedNodes)
	if err != nil {
		err := fmt.Errorf("calcScore failed %v for pod %v", err, args.Pod.Name)
		s.recordScheduleFilterResultEvent(args.Pod, EventReasonFilteringFailed, "", err)
		return nil, err
	}
	if len((*nodeScores).NodeList) == 0 {
		klog.V(4).InfoS("No available nodes meet the required scores",
			"pod", args.Pod.Name)
		s.recordScheduleFilterResultEvent(args.Pod, EventReasonFilteringFailed, "", fmt.Errorf("no available node, %d nodes do not meet", len(*args.NodeNames)))
		return &extenderv1.ExtenderFilterResult{
			FailedNodes: failedNodes,
		}, nil
	}
	klog.V(4).Infoln("nodeScores_len=", len((*nodeScores).NodeList))
	sort.Sort(nodeScores)
	m := (*nodeScores).NodeList[len((*nodeScores).NodeList)-1]
	klog.InfoS("Scheduling pod to node",
		"podNamespace", args.Pod.Namespace,
		"podName", args.Pod.Name,
		"nodeID", m.NodeID,
		"devices", m.Devices)
	annotations := make(map[string]string)
	annotations[util.AssignedNodeAnnotations] = m.NodeID
	annotations[util.AssignedTimeAnnotations] = strconv.FormatInt(time.Now().Unix(), 10)

	for _, val := range device.GetDevices() {
		val.PatchAnnotations(args.Pod, &annotations, m.Devices)
	}

	if s.podManager.AddPod(args.Pod, m.NodeID, m.Devices) {
		s.quotaManager.AddUsage(args.Pod, m.Devices)
	}
	err = util.PatchPodAnnotations(args.Pod, annotations)
	if err != nil {
		s.recordScheduleFilterResultEvent(args.Pod, EventReasonFilteringFailed, "", err)
		s.podManager.DelPod(args.Pod)
		return nil, err
	}
	successMsg := genSuccessMsg(len(*args.NodeNames), m.NodeID, nodeScores.NodeList)
	s.recordScheduleFilterResultEvent(args.Pod, EventReasonFilteringSucceed, successMsg, nil)
	res := extenderv1.ExtenderFilterResult{NodeNames: &[]string{m.NodeID}}
	return &res, nil
}

func genSuccessMsg(totalNodes int, target string, nodes []*policy.NodeScore) string {
	successMsg := "find fit node(%s), %d nodes not fit, %d nodes fit(%s)"
	var scores []string
	for _, no := range nodes {
		scores = append(scores, fmt.Sprintf("%s:%.2f", no.NodeID, no.Score))
	}
	score := strings.Join(scores, ",")
	return fmt.Sprintf(successMsg, target, totalNodes-len(nodes), len(nodes), score)
}
