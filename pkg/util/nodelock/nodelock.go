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

// Package nodelock 实现了节点级别的分布式锁机制，用于防止并发调度导致的设备资源超分配问题。
//
// # 为什么需要节点锁？
//
// HAMi Scheduler 支持多副本部署（通过 Kubernetes Leader Election），多个 Scheduler 实例可能同时运行。
// 即使启用了 Leader Election，在 Leader 切换期间或网络分区时，仍可能出现多个实例同时调度的情况。
//
// 并发调度场景：
//
// 1. 多副本 Scheduler（Leader Election）
//   - 正常情况：只有 Leader 实例处理调度请求
//   - 异常情况：Leader 切换期间，新旧 Leader 可能同时工作
//   - 网络分区：不同分区的实例可能都认为自己是 Leader
//
// 2. 单个 Scheduler 的并发请求
//   - Scheduler 是无状态的，可以并发处理多个 Pod 的调度请求
//   - 多个 Pod 可能同时被调度到同一个节点
//
// 3. Scheduler 重启或故障恢复
//   - Scheduler 重启后，可能有未完成的设备分配
//   - 需要清理或接管之前的锁
//
// # 问题示例：没有节点锁的情况
//
//	时间线：
//	T1: Scheduler-A 读取 Node-1 的 GPU-0：剩余 8GB 显存
//	T2: Scheduler-A 决定将 Pod-1 分配到 GPU-0（需要 6GB）
//	T3: Scheduler-B 也读取 Node-1 的 GPU-0：剩余 8GB 显存（过期数据）
//	T4: Scheduler-B 决定将 Pod-2 也分配到 GPU-0（需要 6GB）
//	T5: Device Plugin 收到 Pod-1 的 Allocate 请求
//	T6: Device Plugin 收到 Pod-2 的 Allocate 请求
//	结果：GPU-0 被分配了 12GB，但实际只有 8GB！❌
//
// # 节点锁的工作原理
//
// 1. 锁的存储：使用 Kubernetes Node 的 Annotation 作为分布式锁
//   - 注解 Key：hami.io/mutex.lock
//   - 注解 Value：{时间戳},{命名空间},{Pod名称}
//   - 例如：2024-02-21T10:30:00Z,default,training-job-abc123
//
// 2. 锁的生命周期：
//   - 获取锁：Scheduler 在 Filter 阶段前调用 LockNode()
//   - 持有锁：Scheduler 计算设备分配方案，写入 Pod 注解
//   - 释放锁：Device Plugin 在 Allocate 完成后调用 ReleaseNodeLock()
//
// 3. 锁的超时机制：
//   - 默认超时：5 分钟（可通过环境变量 HAMI_NODELOCK_EXPIRE 配置）
//   - 超时后自动释放，防止死锁
//   - 僵尸锁处理：如果持有锁的 Pod 已删除，自动释放
//
// 4. 两层锁机制：
//   - 进程内锁：nodeLockManager 管理每个节点的 sync.Mutex（避免同一进程内的并发）
//   - 分布式锁：Node Annotation（避免不同进程/实例的并发）
//
// # 实际调用流程（基于真实代码）
//
// 1. Scheduler Bind 阶段获取锁（pkg/scheduler/scheduler.go:781-787）
//
//	func (s *Scheduler) Bind(args extenderv1.ExtenderBindingArgs) (*extenderv1.ExtenderBindingResult, error) {
//	    // 获取 Pod 和 Node 对象
//	    current, _ := s.kubeClient.CoreV1().Pods(args.PodNamespace).Get(...)
//	    node, _ := s.kubeClient.CoreV1().Nodes().Get(...)
//
//	    // 为所有设备类型获取节点锁
//	    for _, val := range device.GetDevices() {
//	        err = val.LockNode(node, current)  // ← 实际调用位置
//	        if err != nil {
//	            goto ReleaseNodeLocks
//	        }
//	    }
//
//	    // 更新 Pod 注解（标记为 allocating）
//	    tmppatch := map[string]string{
//	        util.DeviceBindPhase: "allocating",
//	        util.BindTimeAnnotations: strconv.FormatInt(time.Now().Unix(), 10),
//	    }
//	    util.PatchPodAnnotations(current, tmppatch)
//
//	    // 执行绑定
//	    s.kubeClient.CoreV1().Pods(args.PodNamespace).Bind(...)
//
//	    return &extenderv1.ExtenderBindingResult{Error: ""}, nil
//
//	ReleaseNodeLocks:
//	    // 绑定失败，立即释放锁
//	    for _, val := range device.GetDevices() {
//	        val.ReleaseNodeLock(node, current)
//	    }
//	    return &extenderv1.ExtenderBindingResult{Error: err.Error()}, nil
//	}
//
// 2. Device Plugin Allocate 完成后释放锁（pkg/device-plugin/nvidiadevice/nvinternal/plugin/util.go:440）
//
//	func PodAllocationTrySuccess(nodeName string, devName string, lockName string, pod *corev1.Pod) {
//	    // 重新获取 Pod，检查是否所有设备都已分配
//	    refreshed, _ := client.GetClient().CoreV1().Pods(pod.Namespace).Get(...)
//	    annos := refreshed.Annotations[device.InRequestDevices[devName]]
//
//	    // 如果还有其他设备类型未分配，不释放锁
//	    for _, val := range device.DevicesToHandle {
//	        if strings.Contains(annos, val) {
//	            return  // 还有其他设备，继续持有锁
//	        }
//	    }
//
//	    // 所有设备都已分配完成，释放节点锁
//	    klog.Infof("All devices allocate success, releasing lock")
//	    PodAllocationSuccess(nodeName, pod, lockName)  // ← 实际释放位置
//	}
//
//	func PodAllocationSuccess(nodeName string, pod *corev1.Pod, lockName string) {
//	    // 更新 Pod 注解为 "Success"
//	    newAnnos := map[string]string{util.DeviceBindPhase: util.DeviceBindSuccess}
//	    util.PatchPodAnnotations(pod, newAnnos)
//
//	    // 释放节点锁
//	    nodelock.ReleaseNodeLock(nodeName, lockName, pod, false)
//	}
//
// # 注意事项
//
// 1. 锁的粒度：节点级别（不是全局锁），不同节点可以并发调度
// 2. 锁的持有时间：应尽可能短，避免影响调度吞吐量
// 3. 锁的超时：必须设置合理的超时时间，防止死锁
// 4. 僵尸锁：必须处理 Pod 删除、Scheduler 崩溃等异常情况
package nodelock

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Project-HAMi/HAMi/pkg/util/client"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

const (
	// NodeLockKey 是存储节点锁的 Annotation Key
	// 锁的值格式：{时间戳},{命名空间},{Pod名称}
	// 例如：2024-02-21T10:30:00Z,default,my-pod
	NodeLockKey = "hami.io/mutex.lock"

	// NodeLockSep 是节点锁值的分隔符
	NodeLockSep = ","
)

var (
	// nodeLocks 管理每个节点的进程内锁（sync.Mutex）
	// 这是第一层锁：防止同一个 Scheduler 进程内的并发调度
	// Key: 节点名称, Value: 该节点的互斥锁
	nodeLocks = newNodeLockManager()

	// NodeLockTimeout 是节点锁的全局超时时间
	// 默认值：5 分钟
	// 可通过环境变量 HAMI_NODELOCK_EXPIRE 配置（例如：HAMI_NODELOCK_EXPIRE=3m）
	// 超时后锁会被强制释放，防止死锁
	NodeLockTimeout time.Duration = time.Minute * 5

	// DefaultStrategy 是更新节点注解时的重试策略
	// 由于多个 Scheduler 实例可能同时更新节点注解，需要重试机制
	// Steps: 最多重试 5 次
	// Duration: 每次重试间隔 100ms
	// Factor: 重试间隔不增长（固定 100ms）
	// Jitter: 10% 的随机抖动，避免多个实例同时重试
	DefaultStrategy = wait.Backoff{
		Steps:    5,
		Duration: 100 * time.Millisecond,
		Factor:   1.0,
		Jitter:   0.1,
	}
)

// nodeLockManager 管理每个节点的进程内锁（sync.Mutex）
// 这是两层锁机制的第一层：
// - 第一层（进程内）：nodeLockManager 的 sync.Mutex，防止同一进程内的并发
// - 第二层（分布式）：Node Annotation，防止不同进程/实例的并发
//
// 为什么需要两层锁？
// 1. 进程内锁（sync.Mutex）：
//   - 避免同一个 Scheduler 进程内的多个 goroutine 同时操作同一节点
//   - 性能优化：进程内锁比分布式锁快得多
//   - 减少 API Server 压力：避免频繁读写节点注解
//
// 2. 分布式锁（Node Annotation）：
//   - 避免不同 Scheduler 实例（多副本部署）同时操作同一节点
//   - 持久化：锁信息存储在 etcd 中，Scheduler 重启后仍然有效
//   - 可观测：可以通过 kubectl 查看节点锁状态
//
// 使用场景：
//   - 多副本 Scheduler 部署（Leader Election）
//   - 单个 Scheduler 处理多个并发调度请求
//   - Scheduler 重启或故障恢复
type nodeLockManager struct {
	mu    sync.Mutex             // 保护 locks map 的并发访问
	locks map[string]*sync.Mutex // Key: 节点名称, Value: 该节点的互斥锁
}

// newNodeLockManager 创建一个新的节点锁管理器
// 初始化 locks map，用于存储每个节点的互斥锁
func newNodeLockManager() nodeLockManager {
	return nodeLockManager{
		locks: make(map[string]*sync.Mutex),
	}
}

// getLock 返回指定节点的互斥锁，如果不存在则创建
// 这个方法是线程安全的（通过 m.mu 保护）
//
// 工作原理：
// 1. 加锁保护 locks map 的并发访问
// 2. 检查节点锁是否已存在
// 3. 如果不存在，创建新的 sync.Mutex
// 4. 返回节点锁
//
// 注意：返回的 sync.Mutex 指针在 map 中删除后仍然有效
// 已经持有该锁的 goroutine 不会受到影响
func (m *nodeLockManager) getLock(nodeName string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.locks[nodeName]; !ok {
		m.locks[nodeName] = &sync.Mutex{}
	}
	return m.locks[nodeName]
}

// deleteLock 从管理器中删除指定节点的锁条目
// 这个方法是线程安全的（通过 m.mu 保护）
//
// 注意事项：
// 1. 删除 map 条目不会影响已经持有该锁的 goroutine
// 2. sync.Mutex 对象本身不会被释放（Go GC 会处理）
// 3. 如果有 goroutine 仍持有该锁的指针，它们可以继续使用
//
// 调用时机：
// - 节点从集群中删除时（例如：节点缩容）
// - 避免 locks map 无限增长
func (m *nodeLockManager) deleteLock(nodeName string) {
	m.mu.Lock()
	delete(m.locks, nodeName)
	m.mu.Unlock()
}

// CleanupNodeLock 清理指定节点的进程内锁
// 应该在节点从集群中删除时调用（例如：节点 autoscaler 缩容）
//
// 为什么需要这个函数？
// - 避免 nodeLocks.locks map 无限增长
// - 释放不再需要的内存
//
// 调用时机：
// - Node Informer 的 DeleteFunc 中
// - 节点缩容或下线时
//
// 注意：这只清理进程内锁，不影响节点注解中的分布式锁
func CleanupNodeLock(nodeName string) {
	nodeLocks.deleteLock(nodeName)
}

func init() {
	setupNodeLockTimeout()
}

// setupNodeLockTimeout 从环境变量配置节点锁超时时间
// 环境变量：HAMI_NODELOCK_EXPIRE
// 格式：Go duration 字符串（例如：3m, 5m30s, 1h）
// 默认值：5 分钟
//
// 使用示例：
//
//	export HAMI_NODELOCK_EXPIRE=3m
//	# 或在 Deployment 中配置：
//	env:
//	- name: HAMI_NODELOCK_EXPIRE
//	  value: "3m"
func setupNodeLockTimeout() {
	nodelock := os.Getenv("HAMI_NODELOCK_EXPIRE")
	if nodelock != "" {
		d, err := time.ParseDuration(nodelock)
		if err != nil {
			klog.ErrorS(err, "Failed to parse HAMI_NODELOCK_EXPIRE, using default", "duration", NodeLockTimeout)
		} else {
			NodeLockTimeout = d
			klog.InfoS("Node lock expiration time set from environment variable", "duration", d)
		}
	}
}

// SetNodeLock 在节点上设置锁（内部函数，由 LockNode 调用）
// 这个函数假设调用者已经检查过节点未被锁定
//
// 工作流程：
// 1. 获取节点的进程内锁（第一层锁）
// 2. 检查节点注解中是否已有锁（第二层锁）
// 3. 如果未锁定，使用重试机制写入锁注解
// 4. 锁的值格式：{时间戳},{命名空间},{Pod名称}
//
// 参数：
// - nodeName: 节点名称
// - lockname: 锁的注解 Key（通常是 "hami.io/mutex.lock"）
// - pods: 请求锁的 Pod（用于记录锁的持有者）
//
// 返回值：
// - error: 如果节点已被锁定或写入失败，返回错误
//
// 注意：这个函数会持有进程内锁直到返回
func SetNodeLock(nodeName string, lockname string, pods *corev1.Pod) error {
	// 获取节点的进程内锁（第一层锁）
	// 这确保同一个 Scheduler 进程内不会并发操作同一节点
	nodeLock := nodeLocks.getLock(nodeName)
	nodeLock.Lock()
	defer nodeLock.Unlock()

	ctx := context.Background()
	node, err := client.GetClient().CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// 检查节点是否已被锁定（第二层锁：分布式锁）
	if _, ok := node.Annotations[NodeLockKey]; ok {
		return fmt.Errorf("node %s is locked", nodeName)
	}

	// 使用重试机制写入锁注解
	// 为什么需要重试？
	// - 多个 Scheduler 实例可能同时尝试更新节点注解
	// - 节点的 ResourceVersion 可能已被其他操作更新
	// - 网络抖动或 API Server 临时不可用
	err = retry.OnError(DefaultStrategy, func(err error) bool {
		// 对任何错误都重试（包括冲突、超时等）
		return true
	}, func() error {
		// 重新获取节点（确保 ResourceVersion 是最新的）
		node, err = client.GetClient().CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			klog.ErrorS(err, "Failed to get node when retry to patch", "node", nodeName)
			return err
		}

		// 构造 Patch 数据
		// 格式：{"metadata":{"annotations":{"hami.io/mutex.lock":"2024-02-21T10:30:00Z,default,my-pod"},"resourceVersion":"12345"}}
		patchData := fmt.Sprintf(`{"metadata":{"annotations":{"%s":"%s"},"resourceVersion":"%s"}}`,
			NodeLockKey,
			GenerateNodeLockKeyByPod(pods), // 生成锁的值：时间戳,命名空间,Pod名称
			node.ResourceVersion)           // 使用 ResourceVersion 实现乐观锁

		_, err = client.GetClient().CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
		if err != nil {
			klog.ErrorS(err, "Failed to patch node when retry to patch", "node", nodeName)
			return err
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to set node lock (node=%s, retry strategy=%+v): %w", nodeName, DefaultStrategy, err)
	}

	klog.InfoS("Node lock set", "node", nodeName, "podName", pods.Name)
	return nil
}

// ReleaseNodeLock 释放节点锁
// 这个函数会删除节点注解中的锁信息
//
// 工作流程：
// 1. 获取节点的进程内锁（第一层锁）
// 2. 检查节点注解中的锁是否由当前 Pod 持有
// 3. 如果是，使用重试机制删除锁注解
//
// 参数：
// - nodeName: 节点名称
// - lockname: 锁的注解 Key（通常是 "hami.io/mutex.lock"）
// - pod: 请求释放锁的 Pod
// - skipNodeLockOwnerCheck: 是否跳过锁持有者检查
//   - true: 强制释放锁（用于清理过期锁或僵尸锁）
//   - false: 只有锁的持有者才能释放（正常情况）
//
// 返回值：
// - error: 如果释放失败，返回错误
//
// 调用时机：
// - Device Plugin 的 Allocate 方法完成后
// - Scheduler 调度失败时
// - 清理过期锁或僵尸锁时（skipNodeLockOwnerCheck=true）
//
// 注意：这个函数会持有进程内锁直到返回
func ReleaseNodeLock(nodeName string, lockname string, pod *corev1.Pod, skipNodeLockOwnerCheck bool) error {
	if pod == nil {
		return fmt.Errorf("cannot release node lock: pod is nil")
	}

	// 获取节点的进程内锁（第一层锁）
	nodeLock := nodeLocks.getLock(nodeName)
	nodeLock.Lock()
	defer nodeLock.Unlock()

	ctx := context.Background()
	node, err := client.GetClient().CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// 检查节点是否有锁
	lockStr, ok := node.Annotations[NodeLockKey]
	if !ok {
		// 节点未锁定，直接返回（幂等性）
		return nil
	}

	// 检查锁的持有者是否是当前 Pod
	// 锁的格式：2024-02-21T10:30:00Z,default,my-pod
	// 检查方式：锁的值是否以 ",{namespace},{pod-name}" 结尾
	if !skipNodeLockOwnerCheck && !strings.HasSuffix(lockStr, fmt.Sprintf("%s%s", NodeLockSep, GeneratePodNamespaceName(pod, NodeLockSep))) {
		klog.InfoS("NodeLock is not set by this pod", NodeLockKey, lockStr, "podName", pod.Name, "podNamespace", pod.Namespace)
		return nil
	}

	// 使用重试机制删除锁注解
	err = retry.OnError(DefaultStrategy, func(err error) bool {
		// 对任何错误都重试
		return true
	}, func() error {
		// 重新获取节点（确保 ResourceVersion 是最新的）
		node, err = client.GetClient().CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			klog.ErrorS(err, "Failed to get node when retry to patch", "node", nodeName)
			return err
		}

		// 构造 Patch 数据（删除注解）
		// 格式：{"metadata":{"annotations":{"hami.io/mutex.lock":null},"resourceVersion":"12345"}}
		patchData := fmt.Sprintf(`{"metadata":{"annotations":{"%s":null},"resourceVersion":"%s"}}`,
			NodeLockKey,
			node.ResourceVersion)

		_, err = client.GetClient().CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
		if err != nil {
			klog.ErrorS(err, "Failed to patch node when retry to patch", "node", nodeName)
			return err
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to release node lock (node=%s, retry strategy=%+v): %w", nodeName, DefaultStrategy, err)
	}

	klog.InfoS("Node lock released", "node", nodeName, "podName", pod.Name)
	return nil
}

// LockNode 获取节点锁（对外接口）
// 这是 Scheduler 调用的主要函数，用于在调度 Pod 前锁定节点
//
// 工作流程：
// 1. 检查节点是否已被锁定
// 2. 如果未锁定，直接加锁
// 3. 如果已锁定，检查锁是否过期或持有者已删除
// 4. 如果锁过期或持有者已删除，强制释放旧锁并加新锁
// 5. 如果锁仍然有效，返回错误
//
// 参数：
// - nodeName: 节点名称
// - lockname: 锁的注解 Key（通常是 "hami.io/mutex.lock"）
// - pods: 请求锁的 Pod
//
// 返回值：
// - error: 如果无法获取锁，返回错误
//
// 调用时机：
// - Scheduler 的 Filter 阶段前
// - 在计算设备分配方案之前
//
// 并发场景示例：
//
// 场景 1：正常情况（无并发）
//
//	Scheduler-A: LockNode("node-1", pod-1) -> 成功
//	Scheduler-A: 计算设备分配
//	Scheduler-A: 写入 Pod 注解
//	Device Plugin: Allocate -> ReleaseNodeLock
//	Scheduler-B: LockNode("node-1", pod-2) -> 成功
//
// 场景 2：并发冲突
//
//	Scheduler-A: LockNode("node-1", pod-1) -> 成功
//	Scheduler-B: LockNode("node-1", pod-2) -> 失败（节点已锁定）
//	Scheduler-B: 重新调度 pod-2 到其他节点
//
// 场景 3：锁过期
//
//	Scheduler-A: LockNode("node-1", pod-1) -> 成功
//	[5 分钟后，Scheduler-A 崩溃，锁未释放]
//	Scheduler-B: LockNode("node-1", pod-2) -> 检测到锁过期 -> 强制释放 -> 成功
//
// 场景 4：僵尸锁（Pod 已删除）
//
//	Scheduler-A: LockNode("node-1", pod-1) -> 成功
//	[pod-1 被用户删除]
//	Scheduler-B: LockNode("node-1", pod-2) -> 检测到 pod-1 不存在 -> 强制释放 -> 成功
func LockNode(nodeName string, lockname string, pods *corev1.Pod) error {
	ctx := context.Background()
	node, err := client.GetClient().CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// 检查节点是否已被锁定
	if _, ok := node.Annotations[NodeLockKey]; !ok {
		// 节点未锁定，直接加锁
		return SetNodeLock(nodeName, lockname, pods)
	}

	// 节点已被锁定，解析锁信息
	// 锁的格式：2024-02-21T10:30:00Z,default,my-pod
	lockTime, ns, previousPodName, err := ParseNodeLock(node.Annotations[NodeLockKey])
	if err != nil {
		return err
	}

	var skipOwnerCheck = false

	// 检查 1：锁是否过期？
	// 如果锁的时间超过 NodeLockTimeout（默认 5 分钟），认为锁已过期
	// 这通常发生在：
	// - Scheduler 崩溃，未能释放锁
	// - Device Plugin 崩溃，未能释放锁
	// - 网络分区，导致锁无法释放
	if time.Since(lockTime) > NodeLockTimeout {
		klog.InfoS("Node lock expired", "node", nodeName, "lockTime", lockTime, "timeout", NodeLockTimeout)
		skipOwnerCheck = true // 锁过期，可以强制释放
	} else
	// 检查 2：持有锁的 Pod 是否还存在？（处理僵尸锁）
	// 如果 Pod 已被删除（用户删除、驱逐、完成等），锁应该被释放
	// 这避免了因 Pod 异常退出导致节点永久锁定
	if ns != "" && previousPodName != "" && (ns != pods.Namespace || previousPodName != pods.Name) {
		// 尝试获取持有锁的 Pod
		if _, err := client.GetClient().CoreV1().Pods(ns).Get(ctx, previousPodName, metav1.GetOptions{}); err != nil {
			if !apierrors.IsNotFound(err) {
				// 获取 Pod 失败（非 NotFound 错误），可能是网络问题
				klog.ErrorS(err, "Failed to get pod of NodeLock", "podName", previousPodName, "namespace", ns)
				return err
			}
			// Pod 不存在（NotFound），这是僵尸锁
			klog.InfoS("Previous pod of NodeLock not found, releasing lock", "podName", previousPodName, "namespace", ns, "nodeLock", node.Annotations[NodeLockKey])
			skipOwnerCheck = true // Pod 已删除，可以强制释放
		}
	}

	// 如果锁过期或持有者已删除，强制释放旧锁并加新锁
	if skipOwnerCheck {
		err = ReleaseNodeLock(nodeName, lockname, pods, true) // skipNodeLockOwnerCheck=true
		if err != nil {
			klog.ErrorS(err, "Failed to release node lock", "node", nodeName)
			return err
		}
		return SetNodeLock(nodeName, lockname, pods)
	}

	// 锁仍然有效，无法获取
	// Scheduler 应该重新调度 Pod 到其他节点
	return fmt.Errorf("node %s has been locked within %v", nodeName, NodeLockTimeout)
}

// ParseNodeLock 解析节点锁的值
// 锁的格式：{时间戳},{命名空间},{Pod名称}
// 例如：2024-02-21T10:30:00Z,default,my-pod
//
// 参数：
// - value: 节点锁注解的值
//
// 返回值：
// - lockTime: 锁的创建时间（用于判断是否过期）
// - ns: Pod 的命名空间
// - name: Pod 的名称
// - err: 解析错误
//
// 兼容性：
// - 旧版本格式：只有时间戳（例如：2024-02-21T10:30:00Z）
// - 新版本格式：时间戳,命名空间,Pod名称（例如：2024-02-21T10:30:00Z,default,my-pod）
func ParseNodeLock(value string) (lockTime time.Time, ns, name string, err error) {
	// 检查是否包含分隔符（判断是新版本还是旧版本格式）
	if !strings.Contains(value, NodeLockSep) {
		// 旧版本格式：只有时间戳
		lockTime, err = time.Parse(time.RFC3339, value)
		return lockTime, "", "", err
	}

	// 新版本格式：时间戳,命名空间,Pod名称
	s := strings.Split(value, NodeLockSep)
	if len(s) != 3 {
		return time.Time{}, "", "", fmt.Errorf("malformed lock annotation: expected 3 parts, got %d from %s", len(s), value)
	}

	lockTime, err = time.Parse(time.RFC3339, s[0])
	return lockTime, s[1], s[2], err
}

// GenerateNodeLockKeyByPod 生成节点锁的值
// 格式：{时间戳},{命名空间},{Pod名称}
// 例如：2024-02-21T10:30:00Z,default,my-pod
//
// 参数：
// - pod: 请求锁的 Pod（如果为 nil，只返回时间戳）
//
// 返回值：
// - string: 节点锁的值
//
// 为什么要记录 Pod 信息？
// 1. 调试：可以通过 kubectl 查看哪个 Pod 持有锁
// 2. 僵尸锁检测：可以检查持有锁的 Pod 是否还存在
// 3. 锁持有者验证：释放锁时验证是否是锁的持有者
func GenerateNodeLockKeyByPod(pod *corev1.Pod) string {
	if pod == nil {
		return time.Now().Format(time.RFC3339)
	}
	return fmt.Sprintf("%s%s%s", time.Now().Format(time.RFC3339), NodeLockSep, GeneratePodNamespaceName(pod, NodeLockSep))
}

// GeneratePodNamespaceName 生成 Pod 的完整名称
// 格式：{命名空间}{分隔符}{Pod名称}
// 例如：default,my-pod
//
// 参数：
// - pod: Pod 对象（如果为 nil，返回空字符串）
// - sep: 分隔符（通常是 ","）
//
// 返回值：
// - string: Pod 的完整名称
func GeneratePodNamespaceName(pod *corev1.Pod, sep string) string {
	if pod == nil {
		return ""
	}
	return fmt.Sprintf("%s%s%s", pod.Namespace, sep, pod.Name)
}
